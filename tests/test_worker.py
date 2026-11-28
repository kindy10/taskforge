import asyncio
import uuid
import psycopg

from taskforge.config import POSTGRES_DSN
from taskforge.queue_client import QueueClient
from taskforge.worker import Worker


async def test_worker_processes_job_and_marks_completed():
    """Worker should claim a job, run its handler, update Postgres, and ack Redis."""
    queue_name = f"test_worker_{uuid.uuid4().hex[:8]}"
    client = QueueClient()
    worker = Worker(queue_name=queue_name, concurrency=2, poll_interval=0.05)

    processed_payloads = []
    done_event = asyncio.Event()

    @worker.register("send_welcome")
    async def handle_welcome(payload: dict):
        processed_payloads.append(payload)
        done_event.set()

    job_id = await client.enqueue(
        job_type="send_welcome",
        payload={"email": "intern@example.com"},
        queue_name=queue_name,
    )

    worker_task = asyncio.create_task(worker.start())
    try:
        # Wait up to 3 seconds for the handler to run
        await asyncio.wait_for(done_event.wait(), timeout=3.0)
    finally:
        await worker.stop()
        await worker_task
        await client.close()

    assert processed_payloads == [{"email": "intern@example.com"}]

    # Verify PostgreSQL status is 'completed' and attempts incremented to 1
    async with await psycopg.AsyncConnection.connect(POSTGRES_DSN) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT status, attempts FROM jobs WHERE id = %s;", (job_id,))
            assert await cur.fetchone() == ("completed", 1)


async def test_worker_respects_concurrency_limit():
    """With concurrency=2 and 5 jobs, active execution must never exceed 2."""
    queue_name = f"test_conc_{uuid.uuid4().hex[:8]}"
    client = QueueClient()
    worker = Worker(queue_name=queue_name, concurrency=2, poll_interval=0.02)

    currently_running = 0
    max_observed = 0
    completed_count = 0
    all_done = asyncio.Event()

    @worker.register("slow_task")
    async def handle_slow(_payload: dict):
        nonlocal currently_running, max_observed, completed_count
        currently_running += 1
        max_observed = max(max_observed, currently_running)
        await asyncio.sleep(0.15)
        currently_running -= 1
        completed_count += 1
        if completed_count == 5:
            all_done.set()

    for i in range(5):
        await client.enqueue(job_type="slow_task", payload={"i": i}, queue_name=queue_name)

    worker_task = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(all_done.wait(), timeout=5.0)
    finally:
        await worker.stop()
        await worker_task
        await client.close()

    assert completed_count == 5
    assert max_observed <= 2


async def test_worker_graceful_shutdown_waits_for_inflight_job():
    """Calling stop() mid-job must let the running job finish before exiting."""
    queue_name = f"test_shutdown_{uuid.uuid4().hex[:8]}"
    client = QueueClient()
    worker = Worker(queue_name=queue_name, concurrency=1, poll_interval=0.02)

    started = asyncio.Event()
    finished = False

    @worker.register("long_job")
    async def handle_long(_payload: dict):
        nonlocal finished
        started.set()
        await asyncio.sleep(0.25)
        finished = True

    await client.enqueue(job_type="long_job", payload={}, queue_name=queue_name)

    worker_task = asyncio.create_task(worker.start())
    # Wait til the job is actively running, then trigger shutdown immediately
    await asyncio.wait_for(started.wait(), timeout=3.0)
    await worker.stop()
    await worker_task
    await client.close()

    # The worker must not have exited til the in-flight job completed
    assert finished is True