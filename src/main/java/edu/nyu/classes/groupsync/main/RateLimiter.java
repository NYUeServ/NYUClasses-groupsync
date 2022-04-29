package edu.nyu.classes.groupsync.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RateLimiter {
    private static Logger logger = LoggerFactory.getLogger(RateLimiter.class);

    private long queriesPerTimestep;
    private long timestepMs;

    public RateLimiter(long queriesPerTimestep, long timestepMs) {
        this.queriesPerTimestep = queriesPerTimestep;
        this.timestepMs = timestepMs;
    }

    // Google limits to 1500 queries per 100 seconds by default.  This
    // appears to include the subqueries of batch requests (i.e. a single
    // batch request doesn't just count as one query.)
    private List<Long> times = new ArrayList<>();
    private Map<Long, Long> queryCounts = new HashMap<>();

    // Express an interest in running `count` queries.  Block until that's
    // OK.
    public synchronized void wantQueries(long count) {
        if (count > queriesPerTimestep) {
            throw new RuntimeException("Can't execute that many concurrent queries: " + count);
        }

        while ((queriesInLastTimestep() + count) >= queriesPerTimestep) {
            logger.warn("Waiting for rate limiter to allow another {} queries", count);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
        }

        // OK!
        recordQueries(count);
    }

    private void recordQueries(long count) {
        long now = System.currentTimeMillis() / timestepMs;

        if (times.contains(now)) {
            queryCounts.put(now, queryCounts.get(now) + count);
        } else {
            times.add(now);
            queryCounts.put(now, count);
        }
    }


    private long queriesInLastTimestep() {
        long result = 0;
        long timestepStart = (System.currentTimeMillis() - timestepMs) / timestepMs;

        Iterator<Long> it = times.iterator();
        while (it.hasNext()) {
            long time = it.next();

            if (time < timestepStart) {
                // Time expired.  No longer needed.
                it.remove();
                queryCounts.remove(time);
            } else {
                result += queryCounts.get(time);
            }
        }

        return result;
    }

    public void rateLimitHit() {
        logger.warn("Google rate limit hit!");
    }
}
