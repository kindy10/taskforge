import time
from pathlib import Path

import redis.asyncio as redis

#Resolve the directory containing the .lua files relative to this module

LUA_DIR = Path(__file__).parent /"lua"

class RedisQueueScripts:
    """
    Wrapper around atomic Redis Lua scripts for claiming and acknowledging jobs.
    Using register_script() caches the script's SHA1 hash in Redis for fast execution.
    """
    def __init__(self, redis_client:redis.Redis  )-> None:
        self.redis_client = redis_client
        self._claim_script = self.redis_client.register_script(
            (LUA_DIR / "claim_job.lua").read_text(encoding="utf-8")
        )
        self._ack_script = self.redis_client.register_script(
            (LUA_DIR / "ack_job.lua").read_text(encoding="utf-8")
        )

    @staticmethod
    def _ready_key(queue_name: str) -> str:
        return f"queue:{queue_name}:ready"

    @staticmethod
    def _processing_key(queue_name: str) -> str:
        return f"queue:{queue_name}:processing"

    async def claim_job(
            self,
            queue_name: str = "default",
            lease_duration_ms:int = 30_000

    )->str | None:
        """
        Atomically pop one job from 'ready' and add it to 'processing' with a lease.
        Returns:
            The claimed job_id string, or None if the queue is empty.
        """

        lease_expires_at_ms = int(time.time()*1000) + lease_duration_ms
        job_id = await self._claim_script(
            keys=[self._ready_key(queue_name),self._processing_key(queue_name)],
            args=[lease_expires_at_ms]
        )
        return job_id

    async def ack_job(self, job_id: str, queue_name:str='default') -> bool:
        """
        Atomically remove a completed job from 'processing'.
        Returns:
            True if the job was removed, False if it was no longer in 'processing'.

        """
        removed = await self._ack_script(
            keys=[self._processing_key(queue_name)],
            args=[job_id]
        )
        return bool(removed)
