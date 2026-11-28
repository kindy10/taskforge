import uuid
import pytest
import psycopg

from taskforge.config import POSTGRES_DSN
from taskforge.queue_client import QueueClient


@pytest.fixture
async def client():
    """Create a QueueClient and clean up its Redis connection after each test."""
    qc = QueueClient()
    yield qc
    await qc.close()


async def test_enqueue_immediate_job(client: QueueClient):
    """Immediate jobs should be saved in Postgres and pushed to the Redis ready list."""
    queue_name = f"test_immediate_{uuid.uuid4().hex[:8]}"

    job_id = await client.enqueue(
        job_type="send_email",
        payload={"to": "user@example.com"},
        queue_name=queue_name,
    )

    # Verify job is in Redis ready list
    queued_id = await client.redis_client.rpop(client._ready_key(queue_name))
    assert queued_id == job_id

    # Verify job is persisted in PostgreSQL
    async with await psycopg.AsyncConnection.connect(POSTGRES_DSN) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT status, job_type FROM jobs WHERE id = %s;", (job_id,))
            row = await cur.fetchone()
            assert row == ("pending", "send_email")


async def test_enqueue_delayed_job(client: QueueClient):
    """Delayed jobs should be placed in the Redis delayed ZSET, not the ready list."""
    queue_name = f"test_delayed_{uuid.uuid4().hex[:8]}"

    job_id = await client.enqueue(
        job_type="generate_report",
        payload={"report_id": 42},
        queue_name=queue_name,
        delay_ms=5000,
    )

    # Ready list must be empty
    assert await client.redis_client.llen(client._ready_key(queue_name)) == 0

    # Delayed ZSET must contain our job_id with a timestamp score
    score = await client.redis_client.zscore(client._delayed_key(queue_name), job_id)
    assert score is not None


async def test_idempotency_prevents_duplicates(client: QueueClient):
    """Submitting the same idempotency_key twice must return the same ID and push once."""
    queue_name = f"test_idemp_{uuid.uuid4().hex[:8]}"
    key = f"order_receipt_{uuid.uuid4().hex}"

    first_id = await client.enqueue(
        job_type="charge_card",
        payload={"amount": 2500},
        queue_name=queue_name,
        idempotency_key=key,
    )
    second_id = await client.enqueue(
        job_type="charge_card",
        payload={"amount": 2500},
        queue_name=queue_name,
        idempotency_key=key,
    )

    # Both calls must return the exact same job UUID
    assert first_id == second_id

    # Redis must only have 1 entry in the queue, not 2
    assert await client.redis_client.llen(client._ready_key(queue_name)) == 1