package fr.cirad.tools.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import fr.cirad.tools.query.GroupedExecutor.TaskWrapper;

/**
 * Submits the chunk tasks of one query to an executor, keeping the number of tasks in flight (queued or running)
 * within an {@link AdaptiveConcurrencyLimit}, i.e. at a level tuned at run time from measured task latency.
 *
 * <p>The limit never exceeds the executor's maximum pool size, so a host executor sized by maxQueryThreads_&lt;host&gt;
 * caps it. Since the limit only applies to this query's tasks, concurrent queries sharing a host each adjust to the
 * load the others create.
 */
public class AdaptiveTaskSubmitter {

    private final ExecutorService executor;
    private final String taskGroup;
    private final AdaptiveConcurrencyLimit limit;
    private final List<Future<Void>> futures = new ArrayList<>();

    /** Set by tasks that did no actual work, see {@link #excludeCurrentTaskFromMeasurement()} */
    private static final ThreadLocal<Boolean> UNMEASURED_TASK = new ThreadLocal<>();

    /**
     * To be called from within a submitted task that ends up doing no actual work (e.g. a chunk found empty by a cheap pre-check): its
     * latency is then left out of the tuning. Mixing such near-instant tasks with actual ones would otherwise make the reference latency
     * that of the former, so that any window made mostly of the latter would look congested, whatever the concurrency.
     */
    public static void excludeCurrentTaskFromMeasurement() {
        UNMEASURED_TASK.set(Boolean.TRUE);
    }

    public AdaptiveTaskSubmitter(ExecutorService executor, String taskGroup) {
        this(executor, taskGroup, AdaptiveConcurrencyLimit.DEFAULT_MIN_LIMIT, AdaptiveConcurrencyLimit.DEFAULT_INITIAL_LIMIT);
    }

    /**
     * Creates a submitter that never makes callers wait: tasks are handed to the shared executor as soon as submitted, its size
     * alone bounding how many run at once, and no tuning occurs ({@link #getLimit()} returns Integer.MAX_VALUE). Waiting for a
     * slot would otherwise delay the preparation of the next tasks while the executor may already have free threads. Only meant
     * for a GroupedExecutor: a per-query pool must have its size follow a tuned limit.
     */
    public static AdaptiveTaskSubmitter withoutLimit(GroupedExecutor executor, String taskGroup) {
        return withFixedLimit(executor, taskGroup, Integer.MAX_VALUE);
    }

    /**
     * Creates a submitter that makes callers wait only once nMaxTasksInFlight tasks are queued or running, with no tuning: the
     * shared executor's size bounds how many run at once, and this limit only bounds how many submitted tasks may be held in memory
     * meanwhile (e.g. when each one carries a large list of variant IDs). Only meant for a GroupedExecutor, like {@link #withoutLimit}.
     */
    public static AdaptiveTaskSubmitter withFixedLimit(GroupedExecutor executor, String taskGroup, int nMaxTasksInFlight) {
        return new AdaptiveTaskSubmitter(executor, taskGroup, nMaxTasksInFlight);
    }

    private AdaptiveTaskSubmitter(GroupedExecutor executor, String taskGroup, int nFixedLimit) {
        this.executor = executor;
        this.taskGroup = taskGroup;
        limit = new AdaptiveConcurrencyLimit(taskGroup, nFixedLimit, nFixedLimit, nFixedLimit, null);
    }

    /**
     * @param memoryKey see {@link AdaptiveConcurrencyLimit#AdaptiveConcurrencyLimit(String, int, int, int, String)}:
     * submitters given the same key (e.g. the searches on a given database) start near the limit last reached
     */
    public AdaptiveTaskSubmitter(ExecutorService executor, String taskGroup, String memoryKey) {
        this(executor, taskGroup, AdaptiveConcurrencyLimit.DEFAULT_MIN_LIMIT, AdaptiveConcurrencyLimit.DEFAULT_INITIAL_LIMIT, memoryKey);
    }

    public AdaptiveTaskSubmitter(ExecutorService executor, String taskGroup, int minLimit, int initialLimit) {
        this(executor, taskGroup, minLimit, initialLimit, null);
    }

    public AdaptiveTaskSubmitter(ExecutorService executor, String taskGroup, int minLimit, int initialLimit, String memoryKey) {
        this.executor = executor;
        this.taskGroup = taskGroup;
        int maxLimit = executor instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor) executor).getMaximumPoolSize() : initialLimit;
        limit = new AdaptiveConcurrencyLimit(taskGroup, minLimit, initialLimit, maxLimit, memoryKey) {
            @Override
            protected void onLimitChanged(int newLimit) {
                adjustPoolSize(newLimit);
            }
        };
        adjustPoolSize(limit.getLimit());
    }

    /** A per-query pool queues whatever exceeds its core size, so it must follow the limit */
    private void adjustPoolSize(int newLimit) {
        if (executor instanceof ThreadPoolExecutor && !(executor instanceof GroupedExecutor))
            ((ThreadPoolExecutor) executor).setCorePoolSize(newLimit);
    }

    /**
     * Submits a task, first waiting until fewer tasks than the current limit are in flight.
     *
     * @return the task's future, or null if {@link #cancelAll()} was called. Cancelling it frees the task's
     * slot even if the task never runs.
     */
    public Future<Void> submit(Runnable task) throws InterruptedException {
        if (!limit.acquire())
            return null;

        long submitTime = System.nanoTime();
        TrackedFuture future = new TrackedFuture();
        try {
            future.delegate = executor.submit(new TaskWrapper(taskGroup, () -> {
                try {
                    task.run();
                }
                finally {
                    boolean fMeasured = !Boolean.TRUE.equals(UNMEASURED_TASK.get());
                    UNMEASURED_TASK.remove();
                    future.releaseSlot(fMeasured ? System.nanoTime() - submitTime : null);
                }
            }));
        }
        catch (RuntimeException e) {    // e.g. rejected: the task will never run
            future.releaseSlot(null);
            throw e;
        }
        synchronized (futures) {
            futures.add(future);
        }
        return future;
    }

    /** Number of tasks submitted so far */
    public int getSubmittedCount() {
        synchronized (futures) {
            return futures.size();
        }
    }

    /** Cancels all submitted tasks and makes any further submission a no-op. Safe to call from a task. */
    public void cancelAll() {
        limit.close();
        List<Future<Void>> toCancel;
        synchronized (futures) {
            toCancel = new ArrayList<>(futures);
        }
        for (Future<Void> f : toCancel)
            f.cancel(true);
    }

    /**
     * To be called once all tasks are submitted: lets a {@link GroupedExecutor} purge the task group once
     * drained, then waits for every task to finish.
     *
     * @throws java.util.concurrent.CancellationException if tasks were cancelled
     */
    public void awaitCompletion() throws InterruptedException, ExecutionException {
        if (executor instanceof GroupedExecutor)
            ((GroupedExecutor) executor).shutdown(taskGroup);

        List<Future<Void>> toWaitFor;
        synchronized (futures) {
            toWaitFor = new ArrayList<>(futures);
        }
        for (Future<Void> f : toWaitFor)
            f.get();
    }

    public int getLimit() {
        return limit.getLimit();
    }

    /** Frees the task's slot exactly once, whether the task completes or is cancelled before running */
    private final class TrackedFuture implements Future<Void> {
        private final AtomicBoolean fReleased = new AtomicBoolean(false);
        private volatile Future<?> delegate;

        private void releaseSlot(Long latency) {
            if (fReleased.compareAndSet(false, true))
                limit.release(latency);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean fCancelled = delegate.cancel(mayInterruptIfRunning);
            if (fCancelled)
                releaseSlot(null);
            return fCancelled;
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            delegate.get();
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            delegate.get(timeout, unit);
            return null;
        }
    }
}
