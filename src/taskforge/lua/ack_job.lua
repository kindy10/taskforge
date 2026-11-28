--Atomically acknowledge a completed job by removing it from the processing set.
--- An atomic operation is an operation that cannot be interrupted halfway
-- KEYS[1]: queue:<name>:processing  (Redis ZSET)
-- ARGV[1]: job_id                   (UUID string of the completed job)

-- ZREM returns 1 if the job was present and removed, or 0 if it was already gone
-- (for example, if the worker took too long and the Reaper already reclaimed it).
return redis.call('ZREM', KEYS[1], ARGV[1])
