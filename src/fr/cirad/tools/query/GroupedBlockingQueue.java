package fr.cirad.tools.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import fr.cirad.tools.query.GroupedExecutor.*;

public class GroupedBlockingQueue<E> implements BlockingQueue<E> {

    private Map<String, LinkedBlockingQueue<E>> taskGroups;
    private int previousGroupIndex = -1; // Member variable to track the index of the previously selected group

    public GroupedBlockingQueue() {
        taskGroups = new HashMap<>();
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
        if (group == null) {
            throw new NullPointerException("Group cannot be null");
        }

        Queue<E> groupQueue = taskGroups.computeIfAbsent(group, k -> new LinkedBlockingQueue<>());
        boolean offered = groupQueue.offer(element);
        if (offered) {
            synchronized (this) {
                notifyAll();
            }
            return true;
        }

        return false;
    }

	@Override
    public void put(E element) throws InterruptedException {
        put("", element);
    }
    
    public void put(String group, E element) throws InterruptedException {
        if (group == null) {
            throw new NullPointerException("Group cannot be null");
        }

        LinkedBlockingQueue<E> groupQueue = taskGroups.computeIfAbsent(group, k -> new LinkedBlockingQueue<>());
        groupQueue.put(element);
        synchronized (this) {
            notifyAll();
        }
    }

    @Override
    public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException {
        return offer("", element, timeout, unit);
    }

    public boolean offer(String group, E element, long timeout, TimeUnit unit) throws InterruptedException {
        if (group == null) {
            throw new NullPointerException("Group cannot be null");
        }

        Queue<E> groupQueue = taskGroups.computeIfAbsent(group, k -> new LinkedBlockingQueue<>());
        boolean offered = groupQueue.offer(element);
        if (offered) {
            synchronized (this) {
                notifyAll();
            }
            return true;
        }

        long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
        long remainingTime = unit.toMillis(timeout);

        synchronized (this) {
            while (remainingTime > 0) {
                wait(remainingTime);
                remainingTime = endTime - System.currentTimeMillis();
                offered = groupQueue.offer(element);
                if (offered) {
                    notifyAll();
                    return true;
                }
            }
        }

        return false;
    }

    
    @Override
    public E take() throws InterruptedException {
        synchronized (this) {
	        while (true) {
	            for (int i = 0; i < taskGroups.size(); i++) {
	                int currentIndex = (previousGroupIndex + 1) % taskGroups.size(); // Calculate the index of the next group
	
	                String group = new ArrayList<>(taskGroups.keySet()).get(currentIndex);
	                Queue<E> groupQueue = taskGroups.get(group);
	                if (groupQueue == null) {	// it may have been removed because it ran empty
	                	currentIndex = 0;
		                group = taskGroups.keySet().iterator().next();
		                groupQueue = taskGroups.get(group);
	                }
	                if (groupQueue != null) {
		                E element = groupQueue.poll();
		                if (element != null) {
		                    previousGroupIndex = currentIndex; // Update the previousGroupIndex variable
		                    return element;
		                }
		                else
		                	taskGroups.remove(group);
	                }
	            }
                wait();
            }
        }
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
        long remainingTime = unit.toMillis(timeout);

        while (remainingTime > 0) {
            E element = poll();
            if (element != null) {
                return element;
            }

            synchronized (this) {
                wait(remainingTime);
            }

            remainingTime = endTime - System.currentTimeMillis();
        }

        return null;
    }

    @Override
    public int size() {
        int totalSize = 0;
        for (Queue<E> groupQueue : taskGroups.values()) {
            totalSize += groupQueue.size();
        }
        return totalSize;
    }

    @Override
    public boolean isEmpty() {
        for (Queue<E> groupQueue : taskGroups.values()) {
            if (!groupQueue.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean contains(Object o) {
        for (Queue<E> groupQueue : taskGroups.values()) {
            if (groupQueue.contains(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        List<E> elements = new ArrayList<>();
        for (Queue<E> groupQueue : taskGroups.values()) {
            elements.addAll(groupQueue);
        }
        return elements.iterator();
    }

    @Override
    public Object[] toArray() {
        List<E> elements = new ArrayList<>();
        for (Queue<E> groupQueue : taskGroups.values()) {
            elements.addAll(groupQueue);
        }
        return elements.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        List<E> elements = new ArrayList<>();
        for (Queue<E> groupQueue : taskGroups.values()) {
            elements.addAll(groupQueue);
        }
        return elements.toArray(a);
    }

    @Override
    public boolean remove(Object o) {
        for (Queue<E> groupQueue : taskGroups.values()) {
            if (groupQueue.remove(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object element : c) {
            if (!contains(element)) {
                return false;
            }
        }
        return true;
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
        for (Queue<E> groupQueue : taskGroups.values()) {
            groupQueue.clear();
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

    @Override
    public Spliterator<E> spliterator() {
        List<E> elements = new ArrayList<>();
        for (Queue<E> groupQueue : taskGroups.values()) {
            elements.addAll(groupQueue);
        }
        return elements.spliterator();
    }

    @Override
	public boolean removeIf(Predicate<? super E> filter) {
	    boolean removed = false;
	    for (Queue<E> groupQueue : taskGroups.values()) {
	        removed |= groupQueue.removeIf(filter);
	    }
	    return removed;
	}

	public E poll() {
        for (Queue<E> groupQueue : taskGroups.values()) {
            E element = groupQueue.poll();
            if (element != null) {
                return element;
            }
        }
        return null;
    }

    public E peek() {
        for (Queue<E> groupQueue : taskGroups.values()) {
            E element = groupQueue.peek();
            if (element != null) {
                return element;
            }
        }
        return null;
    }

    @Override
    public E remove() {
        for (Queue<E> groupQueue : taskGroups.values()) {
            E element = groupQueue.poll();
            if (element != null) {
                return element;
            }
        }
        throw new NoSuchElementException();
    }

    public E element() {
        for (Queue<E> groupQueue : taskGroups.values()) {
            E element = groupQueue.peek();
            if (element != null) {
                return element;
            }
        }
        throw new NoSuchElementException();
    }
}