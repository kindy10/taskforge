--An atomic is an indivisible operation that cannot be interrupted by other threads
--Veni Vidi Vici
--
--KEYS[1]: queue:<name>:ready  (Redis List --FIFO queue of pending job IDs)
--KEYS[2]: queue:<name>:processing (Redis ZSET - active job IDs scored by lease_expires_at_ms)
--ARGV[1]: lease_expires_at_ms  (Unix timestamp in ms when this lease expires)

--Pop the oldest job from the tail of the list (matches LPUSH in QueueClient)
local job_id = redis.call('RPOP', KEYS[1])

--if the queue is empty, return nil immediately
if not job_id then
	return nil
end

--Add the job to the processing ZSET with its expiration timestamp as the score
redis.call('ZADD',KEYS[2],ARGV[1],job_id)

return job_id
