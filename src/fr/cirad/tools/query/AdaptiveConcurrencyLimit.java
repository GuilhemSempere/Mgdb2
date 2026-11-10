package fr.cirad.tools.query;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Limits how many operations of a given kind (e.g. one query's chunk queries, one import's chunk writes) may run at
 * once, adjusting that limit at run time from measured operation latency.
 *
 * <p>The tuning follows TCP Vegas: over windows of at least as many completions as the current limit, it compares the
 * average latency with the lowest average seen so far (both allowing for noise, see {@link #NOISE_Z}), and from that
 * ratio estimates how many of the operations in
 * flight are merely waiting (in the server or in some queue) rather than being processed. Too few waiting means the
 * server can take more, so the limit grows; too many means it is saturated, so the limit shrinks. Starting low lets it
 * observe uncongested latency first, which also keeps an operation that starts while others run from overloading the
 * server.
 *
 * <p>Growing by a quarter per window would take some 16 windows to go from 5 to 120, i.e. longer than many queries
 * last, so tuning starts with a slow-start phase (as in TCP): the limit doubles after each window until waiting
 * operations first show up, then moves by quarters. If that first sign of waiting already exceeds the shrinking
 * threshold, the last doubling overshot and is undone.
 *
 * <p>Limits created with a memory key (e.g. one per database for search queries) start from half the limit last
 * reached under the same key, if higher than the initial one, so that successive operations of the same kind need
 * not ramp up from scratch. Starting at half rather than at the full value keeps the first window, which sets the
 * reference latency, likely uncongested.
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

    /**
     * Number of standard errors by which a window's average latency is widened before comparing it with the
     * reference. Operations are seldom alike (chunks holding more or fewer matching variants, some even skipped at
     * once), so window averages are noisy: taking their plain minimum as the reference would bias it low, and make
     * ordinary windows look congested. The reference is thus the lowest upper bound (average + NOISE_Z standard
     * errors) of any window, and a window only shows waiting operations if its lower bound exceeds it.
     */
    static final double NOISE_Z = 2;

    /**
     * Fraction of the level at which congestion was last detected, above which the limit only grows by a sixteenth per window
     * instead of a quarter: otherwise it keeps overshooting that level by a quarter, losing throughput until the next decrease
     */
    static final double DAMPING_ZONE = .85;

    /** Last limit reached by the limits created with a given memory key */
    private static final Map<String, Integer> lastLimitByMemoryKey = new ConcurrentHashMap<>();

    private final String name, memoryKey;
    private final int minLimit, maxLimit;

    private int limit, inFlight = 0, completedInWindow = 0;
    private double latencySumInWindow = 0, latencySquareSumInWindow = 0, referenceLatency = Double.MAX_VALUE;
    private boolean closed = false, slowStart = true;

    /** Level at which waiting operations were last found to exceed the shrinking threshold (Integer.MAX_VALUE if none) */
    private int congestedLimit = Integer.MAX_VALUE;

    /**
     * When the limit was last lowered: operations started before then ran under the previous, higher concurrency, so
     * they are left out of measurement (otherwise, as no new operation starts until enough of them complete, the whole
     * next window would be made of them, and the lowered limit would look as congested as the previous one)
     */
    private long lastDecreaseTime = Long.MIN_VALUE;

    public AdaptiveConcurrencyLimit(String name, int maxLimit) {
        this(name, DEFAULT_MIN_LIMIT, DEFAULT_INITIAL_LIMIT, maxLimit);
    }

    public AdaptiveConcurrencyLimit(String name, int minLimit, int initialLimit, int maxLimit) {
        this(name, minLimit, initialLimit, maxLimit, null);
    }

    /**
     * @param memoryKey if not null, the limit starts from half the one last reached by limits created with the same
     * key (when higher than initialLimit), and records its own for the next ones
     */
    public AdaptiveConcurrencyLimit(String name, int minLimit, int initialLimit, int maxLimit, String memoryKey) {
        this.name = name;
        this.memoryKey = memoryKey;
        this.maxLimit = Math.max(1, maxLimit);
        this.minLimit = Math.max(1, Math.min(minLimit, this.maxLimit));
        Integer lastLimit = memoryKey == null ? null : lastLimitByMemoryKey.get(memoryKey);
        if (lastLimit != null)
            initialLimit = Math.max(initialLimit, lastLimit / 2);
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
        if (closed || latency == null || minLimit == maxLimit || nanoTime() - latency < lastDecreaseTime)	// nothing to tune if the limit is fixed
            return;

        latencySumInWindow += latency;
        latencySquareSumInWindow += (double) latency * latency;
        if (++completedInWindow < Math.max(limit, MIN_WINDOW_SIZE))
            return;

        int n = completedInWindow;
        double averageLatency = latencySumInWindow / n;
        double variance = Math.max(0, (latencySquareSumInWindow - n * averageLatency * averageLatency) / (n - 1));
        double margin = NOISE_Z * Math.sqrt(variance / n), lowerBound = averageLatency - margin;
        latencySumInWindow = 0;
        latencySquareSumInWindow = 0;
        completedInWindow = 0;
        referenceLatency = Math.min(referenceLatency, averageLatency + margin);

        // no waiting operations unless this window is slower than the reference by more than the noise
        double waitingOperations = lowerBound <= referenceLatency ? 0 : limit * (1 - referenceLatency / lowerBound), scale = Math.max(1, Math.log10(limit));
        int previousLimit = limit;
        if (slowStart) {
            if (waitingOperations < ALPHA * scale)
                limit = Math.min(maxLimit, limit * 2);
            else {
                slowStart = false;
                if (waitingOperations > BETA * scale) {
                    congestedLimit = limit;
                    limit = Math.max(minLimit, limit / 2);
                }
            }
        }
        else {
            int step = Math.max(1, limit / 4);
            if (waitingOperations < ALPHA * scale) {
                if (limit >= congestedLimit * 1.25)
                    congestedLimit = Integer.MAX_VALUE;	// went well beyond the last congestion level without congestion: conditions changed
                limit = Math.min(maxLimit, limit + (limit + step <= congestedLimit * DAMPING_ZONE ? step : Math.max(1, limit / 16)));
            }
            else if (waitingOperations > BETA * scale) {
                congestedLimit = limit;
                limit = Math.max(minLimit, limit - step);
            }
        }

        if (memoryKey != null)
            lastLimitByMemoryKey.put(memoryKey, limit);

        if (limit < previousLimit)
            lastDecreaseTime = nanoTime();

        if (LOG.isTraceEnabled())
            LOG.trace(String.format("%s: %d completions averaging %.0f ms (+/- %.0f, reference %.0f ms): ~%d of %d operations in flight were waiting, %s",
                    name, n, averageLatency / 1e6, margin / 1e6, referenceLatency / 1e6, Math.round(waitingOperations), previousLimit,
                    limit == previousLimit ? "keeping limit" : "moving to " + limit + (slowStart ? " (slow start)" : "")));

        if (limit != previousLimit)
            onLimitChanged(limit);
    }

    /** Makes waiting and further {@link #acquire()} calls return false */
    public synchronized void close() {
        closed = true;
        notifyAll();
    }

    public synchronized int getLimit() {
        return limit;
    }

    /** Clock against which latencies passed to {@link #release(Long)} are measured (overridable for simulations) */
    protected long nanoTime() {
        return System.nanoTime();
    }

    /** Called (with this object's lock held) whenever tuning changes the limit */
    protected void onLimitChanged(int newLimit) {
    }
}
