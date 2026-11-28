import asyncio
import uuid
import pytest

from taskforge.queue_client import QueueClient
from taskforge.redis_scripts import RedisQueueScripts


@pytest.fixture
async def queue_env():
    """Provide both a QueueClient and RedisQueueScripts sharing a Redis connection."""
    client = QueueClient()
    scripts = RedisQueueScripts(client.redis_client)
    yield client, scripts
    await client.close()


async def test_claim_and_ack_job(queue_env):
    """Claiming moves a job from ready -> processing; acking removes it completely."""
    client, scripts = queue_env
    queue_name = f"test_claim_{uuid.uuid4().hex[:8]}"

    job_id = await client.enqueue(
        job_type="resize_image",
        payload={"image_id": 101},
        queue_name=queue_name,
    )

    # Claim the job with a 10-second lease
    claimed_id = await scripts.claim_job(queue_name=queue_name, lease_duration_ms=10_000)
    assert claimed_id == job_id

    # Ready list should now be empty; processing ZSET should hold the job with a score
    assert await client.redis_client.llen(scripts._ready_key(queue_name)) == 0
    score = await client.redis_client.zscore(scripts._processing_key(queue_name), job_id)
    assert score is not None

    # Acknowledge the job; first ack returns True, second ack returns False
    assert await scripts.ack_job(job_id=job_id, queue_name=queue_name) is True
    assert await scripts.ack_job(job_id=job_id, queue_name=queue_name) is False


async def test_claim_on_empty_queue_returns_none(queue_env):
    """Claiming from an empty queue should cleanly return None."""
    _, scripts = queue_env
    queue_name = f"test_empty_{uuid.uuid4().hex[:8]}"

    assert await scripts.claim_job(queue_name=queue_name) is None


async def test_concurrent_claims_never_duplicate(queue_env):
    """10 workers racing for 1 job must result in exactly 1 winner and 9 Nones."""
    client, scripts = queue_env
    queue_name = f"test_race_{uuid.uuid4().hex[:8]}"

    job_id = await client.enqueue(
        job_type="send_invoice",
        payload={"invoice_id": 77},
        queue_name=queue_name,
    )

    # Fire 10 simultaneous claim calls
    results = await asyncio.gather(
        *[scripts.claim_job(queue_name=queue_name) for _ in range(10)]
    )

    winners = [r for r in results if r is not None]
    assert winners == [job_id]