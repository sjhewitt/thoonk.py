"""
    Written by Nathan Fritz and Lance Stout. Copyright 2011 by &yet, LLC.
    Released under the terms of the MIT License
"""

import time
import uuid

from thoonk.feeds import Queue
from thoonk.exceptions import Empty, JobNotClaimed, JobNotStalled,\
    ItemDoesNotExist

class Job(Queue):

    """
    A Thoonk Job is a queue which does not completely remove items
    from the queue until a task completion notice is received.

    Job Item Lifecycle:
        - A job is created using self.put() with the data for the job.
        - The job is moved to a claimed state when a worker retrieves
          the job data from the queue.
        - The worker performs any processing required, and calls
          self.finish() with the job's result data.
        - The job is marked as finished and removed from the queue.

    Alternative: Job Cancellation
        - After a worker has claimed a job, it calls self.cancel() with
          the job's ID, possibly because of an error or lack of required
          resources.
        - The job is moved from a claimed state back to the queue.

    Alternative: Job Stalling
        - A call to self.stall() with the job ID is made.
        - The job is moved out of the queue and into a stalled state. While
          stalled, the job will not be dispatched.
        - A call to self.retry() with the job ID is made.
        - The job is moved out of the stalled state and back into the queue.

    Alternative: Job Deletion
        - A call to self.retract() with the job ID is made.
        - The job item is completely removed from the queue and any
          other job states.

    Redis Keys Used:
        feed.published:[feed] -- A time sorted set of queued jobs.
        feed.cancelled:[feed] -- A hash table of cancelled jobs.
        feed.claimed:[feed]   -- A hash table of claimed jobs.
        feed.stalled:[feed]   -- A hash table of stalled jobs.
        feed.running:[feed]   -- A hash table of running jobs.
        feed.publishes:[feed] -- A count of the number of jobs published
        feed.finishes:[feed]  -- A count of the number of jobs finished
        job.finish:[feed]    -- A pubsub channel for job results

    Thoonk.py Implementation API:
        get_schemas   -- Return the set of Redis keys used by this feed.

    Thoonk Standard API:
        cancel      -- Move a job from a claimed state back into the queue.
        finish      -- Mark a job as completed and store the results.
        get         -- Retrieve the next job from the queue.
        get_ids     -- Return IDs of all jobs in the queue.
        get_result  -- Retrieve the result of a job.
        maintenance -- Perform periodic house cleaning.
        put         -- Add a new job to the queue.
        retract     -- Completely remove a job from use.
        retry       -- Resume execution of a stalled job.
        stall       -- Pause execution of a queued job.
    """

    def __init__(self, thoonk, feed):
        """
        Create a new Job queue object for a given Thoonk feed.

        Note: More than one Job queue objects may be create for
              the same Thoonk feed, and creating a Job queue object
              does not automatically generate the Thoonk feed itself.

        Arguments:
            thoonk -- The main Thoonk object.
            feed   -- The name of the feed.
            config -- Optional dictionary of configuration values.
        """
        Queue.__init__(self, thoonk, feed)
        
        self.feed_publishes = 'feed.publishes:%s' % feed
        self.feed_published = 'feed.published:%s' % feed
        self.feed_cancelled = 'feed.cancelled:%s' % feed
        self.feed_retried = 'feed.retried:%s' % feed
        self.feed_finishes = 'feed.finishes:%s' % feed
        self.feed_claimed = 'feed.claimed:%s' % feed
        self.feed_stalled = 'feed.stalled:%s' % feed
        self.feed_running = 'feed.running:%s' % feed
        
        self.job_finish = 'job.finish:%s' % feed        

    def get_channels(self):
        return (self.feed_publishes, self.feed_claimed, self.feed_stalled,
            self.feed_finishes, self.feed_cancelled, self.feed_retried)

    def get_schemas(self):
        """Return the set of Redis keys used exclusively by this feed."""
        schema = set((self.feed_claimed,
                      self.feed_stalled,
                      self.feed_running,
                      self.feed_publishes,
                      self.feed_cancelled))
        return schema.union(Queue.get_schemas(self))

    def get_ids(self):
        """Return the set of IDs used by jobs in the queue."""
        return self.redis.hkeys(self.feed_items)

    def retract(self, id):
        """
        Completely remove a job from use.

        Arguments:
            id -- The ID of the job to remove.
        """
        success = self.redis.evalsha(self.thoonk.scripts["jobs/retract"], 0, self.feed, id)
        if not success:
            raise ItemDoesNotExist

    def put(self, item, priority=False):
        """
        Add a new job to the queue.

        (Same as self.publish())

        Arguments:
            item     -- The content to add to the queue (string).
            priority -- Optional priority; if equal to True then
                        the item will be inserted at the head of the
                        queue instead of the end.
        """
        id = uuid.uuid4().hex
        added = self.redis.evalsha(self.thoonk.scripts["jobs/publish"], 0,
            self.feed, id, item, int(time.time()*1000), 1 if priority else None)
        if added:
            # If zadd was successful
            self.thoonk._publish(self.feed_publishes, (id, item))
        else:
            self.thoonk._publish(self.feed_edit, (id, item))
        return id

    def get(self, timeout=0):
        """
        Retrieve the next job from the queue.

        Raises an Empty exception if the request times out.

        Arguments:
            timeout -- Optional time in seconds to wait before
                       raising an exception.
        
        Returns:
            id      -- The id of the job
            job     -- The job content
        """
        id = self.redis.brpop(self.feed_ids, timeout)
        if id is None:
            raise Empty
        id = id[1]
        result = self.redis.evalsha(self.thoonk.scripts["jobs/get"], 0, self.feed, id, 
            int(time.time()*1000))
        return id, result[1]

    def get_failure_count(self, id):
        return int(self.redis.hget(self.feed_cancelled, id) or 0)
    
    NO_RESULT = []
    def finish(self, id, result=NO_RESULT):
        """
        Mark a job as completed, and store any results.

        Arguments:
            id      -- The ID of the completed job.
            result  -- The result data from the job. (should be a string!)
        """
        success = self.redis.evalsha(self.thoonk.scripts["jobs/finish"], 0, self.feed, id, 
            *([result] if result is not self.NO_RESULT else []))
        if not success:
            raise JobNotClaimed

    def cancel(self, id):
        """
        Move a claimed job back to the queue.

        Arguments:
            id -- The ID of the job to cancel.
        """
        success = self.redis.evalsha(self.thoonk.scripts["jobs/cancel"], 0, self.feed, id)
        if not success:
            raise JobNotClaimed

    def stall(self, id):
        """
        Move a job out of the queue in order to pause processing.

        While stalled, a job will not be dispatched to requesting workers.

        Arguments:
            id -- The ID of the job to pause.
        """
        success = self.redis.evalsha(self.thoonk.scripts["jobs/stall"], 0, self.feed, id)
        if not success:
            raise JobNotClaimed

    def retry(self, id):
        """
        Move a job from a stalled state back into the job queue.

        Arguments:
            id -- The ID of the job to resume.
        """
        success = self.redis.evalsha(self.thoonk.scripts["jobs/retry"], 0, self.feed, id,
            int(time.time()*1000))
        if not success:
            raise JobNotStalled

    def maintenance(self):
        """
        Perform periodic house cleaning.

        Fix any inconsistencies such as jobs that are not in any state, etc,
        that can be caused by software crashes and other unexpected events.

        Expected use is to create a maintenance thread for periodically
        calling this method.
        """
        pipe = self.redis.pipeline()
        pipe.hkeys(self.feed_items)
        pipe.lrange(self.feed_ids)
        pipe.zrange(self.feed_claimed, 0, -1)
        pipe.stall = pipe.smembers(self.feed_stalled)

        keys, avail, claim, stall = pipe.execute()

        unaccounted = [key for key in keys if (key not in avail and \
                                               key not in claim and \
                                               key not in stall)]
        for key in unaccounted:
            self.redis.lpush(self.feed_ids, key)
