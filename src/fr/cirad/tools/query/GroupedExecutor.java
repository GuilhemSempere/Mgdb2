package fr.cirad.tools.query;

import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GroupedExecutor extends ThreadPoolExecutor {
	
	public GroupedExecutor(int poolSize) {
		super(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new GroupedBlockingQueue<>(), Executors.defaultThreadFactory());
	}

    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new GroupedFutureTask<T>(runnable, value);
    }
    
	public void shutdown(String group) {
		((GroupedBlockingQueue) getQueue()).shutdown(group);
	}
	
	@Override
	public void setCorePoolSize(int n) {
		throw new IllegalArgumentException("setCorePoolSize is disabled in " + this.getClass().getSimpleName());
	}
    
    static public class GroupedFutureTask<V> extends FutureTask<V> implements Runnable {
        private Runnable task;

        public GroupedFutureTask(Runnable runnable, V result) {
            super(runnable, result);
            this.task = runnable;
        }

        public Runnable getTask() {
            return task;
        }
    }

    static public class TaskWrapper implements Runnable {
        private final String group;
        private final Runnable task;

        public TaskWrapper(String group, Runnable task) {
            this.group = group;
            this.task = task;
        }
        
        public String getGroup() {
        	return group;
        }

        @Override
        public void run() {
            task.run();
        }
    }
}