import asyncio
import logging
import signal
import socket
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import psycopg
import redis.asyncio as redis

from taskforge.config import POSTGRES_DSN, REDIS_URL
from taskforge.redis_scripts import RedisQueueScripts

logger = logging.getLogger(__name__)

# Type alias for async job handler functions: async def my_handler(payload: dict) -> None
JobHandler = Callable[[dict[str, Any]], Awaitable[Any]]


class Worker:
    """
    Async background worker that claims jobs from Redis, executes registered handlers
    with bounded concurrency, and updates job status in PostgreSQL.
    """

    def __init__(
        self,
        queue_name: str = "default",
        concurrency: int = 5,
        poll_interval: float = 0.1,
        lease_duration_ms: int = 30_000,
        worker_id: str | None = None,
        redis_url: str = REDIS_URL,
        postgres_dsn: str = POSTGRES_DSN,
    ) -> None:
        self.queue_name = queue_name
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.lease_duration_ms = lease_duration_ms
        # Unique identifier for logging and PostgreSQL audit tracking
        self.worker_id = worker_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self.redis_url = redis_url
        self.postgres_dsn = postgres_dsn

        self.redis_client: redis.Redis = redis.from_url(
            self.redis_url, decode_responses=True
        )
        self.scripts = RedisQueueScripts(self.redis_client)

        # Maps job_type strings (e.g., "send_email") to async Python functions
        self._handlers: dict[str, JobHandler] = {}

        # Semaphore caps the maximum number of jobs executing at the same time
        self._semaphore = asyncio.Semaphore(self.concurrency)

        # Tracks currently running asyncio Tasks so graceful shutdown can await them
        self._active_tasks: set[asyncio.Task[None]] = set()

        # Event signaled when the worker should stop claiming new jobs
        self._stop_event = asyncio.Event()

    def register(self, job_type: str) -> Callable[[JobHandler], JobHandler]:
        """Decorator to register an async handler for a specific job_type."""

        def decorator(func: JobHandler) -> JobHandler:
            self._handlers[job_type] = func
            return func

        return decorator

    def install_signal_handlers(self) -> None:
        """
        Register SIGINT / SIGTERM handlers for graceful shutdown.
        Uses standard signal.signal() so it works on both Windows and Linux/Docker.
        """

        def _handle_exit(signum: int, _frame: Any) -> None:
            logger.info("Worker %s received signal %s; shutting down...", self.worker_id, signum)
            self._stop_event.set()

        signal.signal(signal.SIGINT, _handle_exit)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _handle_exit)

    async def start(self) -> None:
        """
        Main polling loop.
        Acquires a semaphore slot BEFORE claiming from Redis so we never pull
        more jobs into memory than our concurrency limit allows.
        """
        logger.info(
            "Worker %s started (queue=%s, concurrency=%d)",
            self.worker_id,
            self.queue_name,
            self.concurrency,
        )

        try:
            while not self._stop_event.is_set():
                # Wait until a concurrency slot is free
                await self._semaphore.acquire()

                # Re-check stop_event in case shutdown was triggered while waiting
                if self._stop_event.is_set():
                    self._semaphore.release()
                    break

                # Atomically claim one job from Redis
                job_id = await self.scripts.claim_job(
                    queue_name=self.queue_name,
                    lease_duration_ms=self.lease_duration_ms,
                )

                if job_id is None:
                    # Queue is empty; release slot and wait briefly before polling again
                    self._semaphore.release()
                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(), timeout=self.poll_interval
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue

                # Spawn a background task to run the job without blocking the poll loop
                task = asyncio.create_task(self._run_job(job_id))
                self._active_tasks.add(task)
                task.add_done_callback(self._active_tasks.discard)
        finally:
            await self.shutdown()

    async def stop(self) -> None:
        """Signal the worker to stop claiming new jobs."""
        self._stop_event.set()

    async def shutdown(self) -> None:
        """Wait for all in-flight jobs to finish, then close Redis connections."""
        if self._active_tasks:
            logger.info(
                "Worker %s waiting for %d in-flight job(s) to finish...",
                self.worker_id,
                len(self._active_tasks),
            )
            await asyncio.gather(*self._active_tasks, return_exceptions=True)

        await self.redis_client.aclose()
        logger.info("Worker %s shut down cleanly.", self.worker_id)

    async def _run_job(self, job_id: str) -> None:
        """Fetch job metadata from Postgres, execute the handler, and record outcome."""
        try:
            # 1. Mark job as 'processing' and load its type + payload from PostgreSQL
            async with await psycopg.AsyncConnection.connect(self.postgres_dsn) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE jobs
                        SET status = 'processing',
                            worker_id = %s,
                            attempts = attempts + 1,
                            updated_at = NOW()
                        WHERE id = %s
                        RETURNING job_type, payload;
                        """,
                        (self.worker_id, job_id),
                    )
                    row = await cur.fetchone()
                await conn.commit()

            if row is None:
                logger.warning("Job %s not found in PostgreSQL; acknowledging.", job_id)
                await self.scripts.ack_job(job_id=job_id, queue_name=self.queue_name)
                return

            job_type, payload = row
            handler = self._handlers.get(job_type)
            if handler is None:
                raise ValueError(f"No handler registered for job_type='{job_type}'")

            # 2. Execute the user's async handler
            await handler(payload)

            # 3. Mark completed in PostgreSQL and remove from Redis processing set
            async with await psycopg.AsyncConnection.connect(self.postgres_dsn) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE jobs
                        SET status = 'completed',
                            last_error = NULL,
                            updated_at = NOW()
                        WHERE id = %s;
                        """,
                        (job_id,),
                    )
                await conn.commit()

            await self.scripts.ack_job(job_id=job_id, queue_name=self.queue_name)

        except Exception as exc:
            logger.exception("Job %s failed on worker %s", job_id, self.worker_id)
            # Record failure in PostgreSQL (Day 7 will add exponential backoff & DLQ routing)
            async with await psycopg.AsyncConnection.connect(self.postgres_dsn) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE jobs
                        SET status = 'retrying',
                            last_error = %s,
                            updated_at = NOW()
                        WHERE id = %s;
                        """,
                        (str(exc), job_id),
                    )
                await conn.commit()
            await self.scripts.ack_job(job_id=job_id, queue_name=self.queue_name)

        finally:
            # Always release the semaphore slot so the worker can claim the next job
            self._semaphore.release()