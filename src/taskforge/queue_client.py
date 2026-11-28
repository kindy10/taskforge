import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import redis.asyncio as redis
from typing import Any
from taskforge.config import POSTGRES_DSN, REDIS_URL
from psycopg.types.json import Jsonb

class QueueClient:
    """
     Producer SDK for submitting background jobs to TaskForge.

    Write order (Durability First):
      1. Insert the job into PostgresSQL with an optional `idempotency_key`.
         If the key already exists, return the existing job_id immediately
         without pushing a duplicate into Redis.
      2. Push the job_id into Redis (`ready` list or `delayed` sorted set).
    """

    def __init__(
       self,
        redis_url:str = REDIS_URL,
        postgres_dsn:str= POSTGRES_DSN
    ) ->None:
        self.redis_url = redis_url
        self.postgres_dsn = postgres_dsn

        #decode_responses= True return Python strings instead of raw bytes
        self.redis_client:redis.Redis = redis.Redis.from_url(
            self.redis_url, decode_responses=True
        )

    async def close(self) -> None:
        """ Close underlying Redis connection pool cleanly."""
        await self.redis_client.aclose()

    @staticmethod
    def _ready_key(queue_name:str)->str:
        """Redis List holding job IDs ready for immediate worker pickup."""
        return f"queue:{queue_name}:ready"

    @staticmethod
    def _delayed_key(queue_name:str)->str:
        """Redis Sorted set holding delayed job IDs scored by execute_at_ms."""
        return f"queue:{queue_name}:delayed"

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any],
        queue_name: str = "default",
        delay_ms: int = 0,
        max_retries: int = 3,
        idempotency_key: str | None = None,
    ) -> str:
        """
        Enqueue a new background job.

        Returns:
            The UUID string of the newly created job (or the existing job's UUID
            if `idempotency_key` was already used).
        """
        if delay_ms < 0:
            raise ValueError("delay_ms must be >= 0")

        job_id = str(uuid.uuid4())
        now_utc = datetime.now(timezone.utc)
        scheduled_at = now_utc + timedelta(milliseconds=delay_ms)

        # Step 1: Persist to PostgreSQL first (and enforce idempotency)
        async with await psycopg.AsyncConnection.connect(self.postgres_dsn) as conn:
            async with conn.cursor() as cur:
                # ON CONFLICT DO NOTHING prevents race conditions when two identical
                # requests arrive at the exact same millisecond.
                await cur.execute(
                    """
                    INSERT INTO jobs (
                        id, queue_name, job_type, idempotency_key,
                        payload, status, max_retries, scheduled_at
                    )
                    VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING id;
                    """,
                    (
                        job_id,
                        queue_name,
                        job_type,
                        idempotency_key,
                        Jsonb(payload),
                        max_retries,
                        scheduled_at,
                    ),
                )
                row = await cur.fetchone()

                if row is None and idempotency_key is not None:
                    # A job with this idempotency_key already exists; fetch and return its ID
                    await cur.execute(
                        "SELECT id FROM jobs WHERE idempotency_key = %s;",
                        (idempotency_key,),
                    )
                    existing = await cur.fetchone()
                    return str(existing[0])

            await conn.commit()

        # Step 2: Push the job ID into Redis for fast worker pickup
        if delay_ms == 0:
            # LPUSH adds to the head; workers will RPOP from the tail (FIFO order)
            await self.redis_client.lpush(self._ready_key(queue_name), job_id)
        else:
            # Score is the Unix timestamp in milliseconds when the job becomes eligible
            execute_at_ms = int(time.time() * 1000) + delay_ms
            await self.redis_client.zadd(
                self._delayed_key(queue_name), {job_id: execute_at_ms}
            )

        return job_id