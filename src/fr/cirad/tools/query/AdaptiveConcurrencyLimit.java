package fr.cirad.tools.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Limits how many operations of a given kind (e.g. one query's chunk queries, one import's chunk writes) may run at
 * once, adjusting that limit at run time from measured operation latency.
 *
 * <p>The tuning follows TCP Vegas: over windows of at least as many completions as the current limit, it compares the
 * average latency with the lowest average seen so far, and from that ratio estimates how many of the operations in
 * flight are merely waiting (in the server or in some queue) rather than being processed. Too few waiting means the
 * server can take more, so the limit grows; too many means it is saturated, so the limit shrinks. Starting low lets it
 * observe uncongested latency first, which also keeps an operation that starts while others run from overloading the
 * server.
 */
public class AdaptiveConcurrencyLimit {

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveConcurrencyLimit.class);

    public static final int DEFAULT_MIN_LIMIT = 2;
    public static final int DEFAULT_INITIAL_LIMIT = 5;

    /** Estimated number of waiting operations (scaled by log10 of the limit) below which the limit grows */
    static final double ALPHA = 3;

    /** Estimated number of waiting operations (scaled by log10 of the limit) above which the limit shrinks */
    static final double BETA = 6;

    /** Minimum number of completions a measurement window contains, to limit noise at low limits */
    static final int MIN_WINDOW_SIZE = 4;

    private final String name;
    private final int minLimit, maxLimit;

    private int limit, inFlight = 0, completedInWindow = 0;
    private double latencySumInWindow = 0, minAverageLatency = Double.MAX_VALUE;
    private boolean closed = false;

    public AdaptiveConcurrencyLimit(String name, int maxLimit) {
        this(name, DEFAULT_MIN_LIMIT, DEFAULT_INITIAL_LIMIT, maxLimit);
    }

    public AdaptiveConcurrencyLimit(String name, int minLimit, int initialLimit, int maxLimit) {
        this.name = name;
        this.maxLimit = Math.max(1, maxLimit);
        this.minLimit = Math.max(1, Math.min(minLimit, this.maxLimit));
        limit = Math.max(this.minLimit, Math.min(this.maxLimit, initialLimit));
    }

    /**
     * Waits until fewer operations than the current limit are in flight, then counts the caller's one in. Every
     * successful call must be matched by a {@link #release(Long)}.
     *
     * @return false if {@link #close()} was called, in which case the operation must not run
     */
    public synchronized boolean acquire() throws InterruptedException {
        while (inFlight >= limit && !closed)
            wait();
        if (closed)
            return false;
        inFlight++;
        return true;
    }

    /** @param latency the operation's latency in nanoseconds, or null if it did not complete (e.g. was cancelled) */
    public synchronized void release(Long latency) {
        inFlight--;
        notifyAll();
        if (closed || latency == null)
            return;

        latencySumInWindow += latency;
        if (++completedInWindow < Math.max(limit, MIN_WINDOW_SIZE))
            return;

        double averageLatency = latencySumInWindow / completedInWindow;
        latencySumInWindow = 0;
        completedInWindow = 0;
        minAverageLatency = Math.min(minAverageLatency, averageLatency);

        double waitingOperations = limit * (1 - minAverageLatency / averageLatency), scale = Math.max(1, Math.log10(limit));
        int previousLimit = limit, step = Math.max(1, limit / 4);
        if (waitingOperations < ALPHA * scale)
            limit = Math.min(maxLimit, limit + step);
        else if (waitingOperations > BETA * scale)
            limit = Math.max(minLimit, limit - step);

        if (limit != previousLimit) {
            LOG.trace(name + ": ~" + Math.round(waitingOperations) + " of " + previousLimit + " operations in flight were waiting, moving to " + limit);
            onLimitChanged(limit);
        }
    }

    /** Makes waiting and further {@link #acquire()} calls return false */
    public synchronized void close() {
        closed = true;
        notifyAll();
    }

    public synchronized int getLimit() {
        return limit;
    }

    /** Called (with this object's lock held) whenever tuning changes the limit */
    protected void onLimitChanged(int newLimit) {
    }
}
