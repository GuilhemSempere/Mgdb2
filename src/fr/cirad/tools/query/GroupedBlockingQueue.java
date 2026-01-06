package fr.cirad.tools.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.log4j.Logger;

import fr.cirad.tools.query.GroupedExecutor.GroupedFutureTask;
import fr.cirad.tools.query.GroupedExecutor.TaskWrapper;

public class GroupedBlockingQueue<E> implements BlockingQueue<E> {

	protected static final Logger LOG = Logger.getLogger(GroupedBlockingQueue.class);
	
	private HashMap<String, LinkedBlockingQueue<E>> taskGroups;
    private HashSet<String> shutdownGroups = new HashSet<>();
    private int previousGroupIndex = -1;
    private final Object lock = new Object(); // Single lock object for all synchronization

    public GroupedBlockingQueue() {
        taskGroups = new HashMap<>();
    }
    
    public int getGroupCount() {
        synchronized (lock) {
            return taskGroups.size();
        }
    }
    
	public void shutdown(String group) {
		synchronized (lock) {
			if (taskGroups.containsKey(group))
				shutdownGroups.add(group);
            lock.notifyAll(); // Wake up threads waiting in take()
		}
	}

    @Override
    public boolean add(E e) {
        if (offer(e))
            return true;
        else
            throw new IllegalStateException("Queue full");
    }

    @Override
    public boolean offer(E element) {
    	String group = GroupedFutureTask.class.equals(element.getClass()) && TaskWrapper.class.equals(((GroupedFutureTask<?>) element).getTask().getClass()) ?
    			((TaskWrapper) ((GroupedFutureTask<?>) element).getTask()).getGroup() : "";

        return offer(group, element);
    }

    public boolean offer(String group, E element) {
        if (group == null)
            throw new NullPointerException("Group cannot be null");
        
        synchronized (lock) {
            if (shutdownGroups.contains(group))
                throw new RejectedExecutionException("Group " + group + " has already been shutdown (" + taskGroups.size() + ")");

            LinkedBlockingQueue<E> groupQueue = taskGroups.computeIfAbsent(group, k -> {
                shutdownGroups.remove(group);
                return new LinkedBlockingQueue<>();
            });
            
            boolean offered = groupQueue.offer(element);
            if (offered) {
                lock.notifyAll();
                return true;
            }
            return false;
        }
    }

	@Override
    public void put(E element) throws InterruptedException {
        put("", element);
    }
    
    public void put(String group, E element) throws InterruptedException {
        if (group == null) {
            throw new NullPointerException("Group cannot be null");
        }

        synchronized (lock) {
            LinkedBlockingQueue<E> groupQueue = taskGroups.computeIfAbsent(group, k -> {
                shutdownGroups.remove(group);
                return new LinkedBlockingQueue<>();
            });
            groupQueue.put(element);
            lock.notifyAll();
        }
    }

    @Override
    public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException {
        return offer("", element, timeout, unit);
    }

    public boolean offer(String group, E element, long timeout, TimeUnit unit) throws InterruptedException {
        if (group == null)
            throw new NullPointerException("Group cannot be null");

        synchronized (lock) {
            LinkedBlockingQueue<E> groupQueue = taskGroups.computeIfAbsent(group, k -> {
                shutdownGroups.remove(group);
                return new LinkedBlockingQueue<>();
            });
            
            boolean offered = groupQueue.offer(element);
            if (offered) {
                lock.notifyAll();
                return true;
            }

            long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
            long remainingTime = unit.toMillis(timeout);

            while (remainingTime > 0) {
                lock.wait(remainingTime);
                remainingTime = endTime - System.currentTimeMillis();
                offered = groupQueue.offer(element);
                if (offered) {
                    lock.notifyAll();
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public E take() throws InterruptedException {
        synchronized (lock) {
            while (true) {
                // First, clean up any empty shutdown groups
                cleanupEmptyShutdownGroups();
                
                List<String> groups = new ArrayList<>(taskGroups.keySet());
                if (groups.isEmpty()) {
                    lock.wait();
                    continue;
                }

                // Start from next group after the last one we took from
                int startIndex = (previousGroupIndex + 1) % groups.size();
                int currentIndex = startIndex;
                
                do {
                    String group = groups.get(currentIndex);
                    LinkedBlockingQueue<E> groupQueue = taskGroups.get(group);
                    
                    if (groupQueue != null) {
                        E element = groupQueue.poll();
                        if (element != null) {
                            previousGroupIndex = currentIndex;
                            // Check if queue is now empty and group is shutdown
                            if (groupQueue.isEmpty() && shutdownGroups.contains(group)) {
                                shutdownGroups.remove(group);
                                taskGroups.remove(group);
                                LOG.debug("Removed empty shutdown group after polling: " + group);
                            }
                            return element;
                        }
                    }
                    
                    currentIndex = (currentIndex + 1) % groups.size();
                } while (currentIndex != startIndex);

                // If we get here, we've checked all groups and found nothing
                // Clean up again before waiting
                cleanupEmptyShutdownGroups();
                lock.wait();
            }
        }
    }
    
    /**
     * Removes all groups that are both empty and marked for shutdown.
     * Must be called while holding the lock.
     */
    private void cleanupEmptyShutdownGroups() {
        if (shutdownGroups.isEmpty()) {
            return;
        }
        
        List<String> groupsToRemove = new ArrayList<>();
        for (String group : shutdownGroups) {
            LinkedBlockingQueue<E> groupQueue = taskGroups.get(group);
            if (groupQueue != null && groupQueue.isEmpty()) {
                groupsToRemove.add(group);
            }
        }
        
        for (String group : groupsToRemove) {
            shutdownGroups.remove(group);
            taskGroups.remove(group);
            LOG.debug("Cleaned up empty shutdown group: " + group + " (remaining groups: " + taskGroups.size() + ")");
        }
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
        long remainingTime = unit.toMillis(timeout);

        synchronized (lock) {
            while (remainingTime > 0) {
                E element = pollInternal();
                if (element != null) {
                    return element;
                }

                lock.wait(remainingTime);
                remainingTime = endTime - System.currentTimeMillis();
            }
            return null;
        }
    }

    @Override
    public int size() {
        synchronized (lock) {
            int totalSize = 0;
            for (Queue<E> groupQueue : taskGroups.values()) {
                totalSize += groupQueue.size();
            }
            return totalSize;
        }
    }

    @Override
    public boolean isEmpty() {
        synchronized (lock) {
            for (Queue<E> groupQueue : taskGroups.values()) {
                if (!groupQueue.isEmpty()) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public boolean contains(Object o) {
        synchronized (lock) {
            for (Queue<E> groupQueue : taskGroups.values()) {
                if (groupQueue.contains(o)) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public Iterator<E> iterator() {
        synchronized (lock) {
            List<E> elements = new ArrayList<>();
            for (Queue<E> groupQueue : taskGroups.values()) {
                elements.addAll(groupQueue);
            }
            return elements.iterator();
        }
    }

    @Override
    public Object[] toArray() {
        synchronized (lock) {
            List<E> elements = new ArrayList<>();
            for (Queue<E> groupQueue : taskGroups.values()) {
                elements.addAll(groupQueue);
            }
            return elements.toArray();
        }
    }

    @Override
    public <T> T[] toArray(T[] a) {
        synchronized (lock) {
            List<E> elements = new ArrayList<>();
            for (Queue<E> groupQueue : taskGroups.values()) {
                elements.addAll(groupQueue);
            }
            return elements.toArray(a);
        }
    }

    @Override
    public boolean remove(Object o) {
        synchronized (lock) {
            for (Queue<E> groupQueue : taskGroups.values()) {
                if (groupQueue.remove(o)) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        synchronized (lock) {
            for (Object element : c) {
                if (!contains(element)) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException("addAll operation is not supported by GroupedBlockingQueue");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("removeAll operation is not supported by GroupedBlockingQueue");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("retainAll operation is not supported by GroupedBlockingQueue");
    }

    @Override
    public void clear() {
        synchronized (lock) {
            shutdownGroups.clear();
            taskGroups.clear();
            lock.notifyAll();
        }
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        synchronized (lock) {
            int elementsCount = 0;
            for (Queue<E> groupQueue : taskGroups.values()) {
                while (elementsCount < maxElements) {
                    E element = groupQueue.poll();
                    if (element == null) {
                        break;
                    }
                    c.add(element);
                    elementsCount++;
                }
            }
            return elementsCount;
        }
    }

    @Override
    public Spliterator<E> spliterator() {
        synchronized (lock) {
            List<E> elements = new ArrayList<>();
            for (Queue<E> groupQueue : taskGroups.values()) {
                elements.addAll(groupQueue);
            }
            return elements.spliterator();
        }
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        synchronized (lock) {
            boolean removed = false;
            for (Queue<E> groupQueue : taskGroups.values()) {
                removed |= groupQueue.removeIf(filter);
            }
            return removed;
        }
    }

    public E poll() {
        synchronized (lock) {
            return pollInternal();
        }
    }

    private E pollInternal() {
        // Must be called while holding lock
        for (Queue<E> groupQueue : taskGroups.values()) {
            E element = groupQueue.poll();
            if (element != null) {
                return element;
            }
        }
        return null;
    }

    public E peek() {
        synchronized (lock) {
            for (Queue<E> groupQueue : taskGroups.values()) {
                E element = groupQueue.peek();
                if (element != null) {
                    return element;
                }
            }
            return null;
        }
    }

    @Override
    public E remove() {
        synchronized (lock) {
            for (Queue<E> groupQueue : taskGroups.values()) {
                E element = groupQueue.poll();
                if (element != null) {
                    return element;
                }
            }
            throw new NoSuchElementException();
        }
    }

    public E element() {
        synchronized (lock) {
            for (Queue<E> groupQueue : taskGroups.values()) {
                E element = groupQueue.peek();
                if (element != null) {
                    return element;
                }
            }
            throw new NoSuchElementException();
        }
    }
    
    public List<Integer> getGroupQueueCounts() {
        synchronized (lock) {
            return taskGroups.values().stream().map(g -> g.size()).toList();
        }
    }
}