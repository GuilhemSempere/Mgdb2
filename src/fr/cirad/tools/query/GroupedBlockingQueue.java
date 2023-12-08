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
    private int previousGroupIndex = -1; // Member variable to track the index of the previously selected group

    public GroupedBlockingQueue() {
        taskGroups = new HashMap<>();
    }
    
	public void shutdown(String group) {
    	/*synchronized (this)*/ {
    		shutdownGroups.add(group);
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
    	/*synchronized (this)*/ {
	        if (group == null)
	            throw new NullPointerException("Group cannot be null");
	        
	    	if (shutdownGroups.contains(group))
	            throw new RejectedExecutionException("Group " + group + " has already been shutdown (" + taskGroups.size() + ")");
	
	        LinkedBlockingQueue<E> groupQueue = taskGroups.computeIfAbsent(group, k -> {
        		shutdownGroups.remove(group);
        		return new LinkedBlockingQueue<>();
        	} );
	        boolean offered = groupQueue.offer(element);
	        if (offered) {
	            synchronized (this) {
	                notifyAll();
	            }
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
    	/*synchronized (this)*/ {
	        if (group == null) {
	            throw new NullPointerException("Group cannot be null");
	        }
	
	        LinkedBlockingQueue<E> groupQueue = taskGroups.computeIfAbsent(group, k -> {
	        		shutdownGroups.remove(group);
	        		return new LinkedBlockingQueue<>();
	        	} );
	        groupQueue.put(element);
//	        /*synchronized (this)*/ {
	            notifyAll();
//	        }
    	}
    }

    @Override
    public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException {
        return offer("", element, timeout, unit);
    }

    public boolean offer(String group, E element, long timeout, TimeUnit unit) throws InterruptedException {
    	/*synchronized (this)*/ {
	        if (group == null)
	            throw new NullPointerException("Group cannot be null");
	
	        LinkedBlockingQueue<E> groupQueue = taskGroups.computeIfAbsent(group, k -> {
	    		shutdownGroups.remove(group);
	    		return new LinkedBlockingQueue<>();
	    	} );
	        boolean offered = groupQueue.offer(element);
	        if (offered) {
	            /*synchronized (this)*/ {
	                notifyAll();
	            }
	            return true;
	        }
	
	        long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
	        long remainingTime = unit.toMillis(timeout);
	
	        /*synchronized (this)*/ {
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
//		                    LOG.debug("Took task from group: " + currentIndex + " / " + taskGroups.get(group).size());
		                    return element;
		                }
		                else if (taskGroups.get(group).isEmpty() && shutdownGroups.contains(group)) {
	                		shutdownGroups.remove(group);
	                		taskGroups.remove(group);
//	                		LOG.debug("Removed group: " + group + " / " + taskGroups.size());
		                }
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

            /*synchronized (this)*/ {
                wait(remainingTime);
            }

            remainingTime = endTime - System.currentTimeMillis();
        }

        return null;
    }

    @Override
    public int size() {
    	/*synchronized (this)*/ {
	        int totalSize = 0;
	        for (Queue<E> groupQueue : taskGroups.values()) {
	            totalSize += groupQueue.size();
	        }
	        return totalSize;
    	}
    }

    @Override
    public boolean isEmpty() {
    	/*synchronized (this)*/ {
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
    	/*synchronized (this)*/ {
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
    	/*synchronized (this)*/ {
	        List<E> elements = new ArrayList<>();
	        for (Queue<E> groupQueue : taskGroups.values()) {
	            elements.addAll(groupQueue);
	        }
	        return elements.iterator();
    	}
    }

    @Override
    public Object[] toArray() {
    	/*synchronized (this)*/ {
	        List<E> elements = new ArrayList<>();
	        for (Queue<E> groupQueue : taskGroups.values()) {
	            elements.addAll(groupQueue);
	        }
	        return elements.toArray();
    	}
    }

    @Override
    public <T> T[] toArray(T[] a) {
    	/*synchronized (this)*/ {
	        List<E> elements = new ArrayList<>();
	        for (Queue<E> groupQueue : taskGroups.values()) {
	            elements.addAll(groupQueue);
	        }
	        return elements.toArray(a);
    	}
    }

    @Override
    public boolean remove(Object o) {
    	/*synchronized (this)*/ {
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
    	/*synchronized (this)*/ {
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
    	/*synchronized (this)*/ {
	    	taskGroups.clear();
	    	shutdownGroups.clear();
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
    	/*synchronized (this)*/ {
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
    	/*synchronized (this)*/ {
	        List<E> elements = new ArrayList<>();
	        for (Queue<E> groupQueue : taskGroups.values()) {
	            elements.addAll(groupQueue);
	        }
	        return elements.spliterator();
    	}
    }

    @Override
	public boolean removeIf(Predicate<? super E> filter) {
    	/*synchronized (this)*/ {
		    boolean removed = false;
		    for (Queue<E> groupQueue : taskGroups.values()) {
		        removed |= groupQueue.removeIf(filter);
		    }
		    return removed;
    	}
	}

	public E poll() {
    	/*synchronized (this)*/ {
	        for (Queue<E> groupQueue : taskGroups.values()) {
	            E element = groupQueue.poll();
	            if (element != null) {
	                return element;
	            }
	        }
	        return null;
    	}
    }

    public E peek() {
    	/*synchronized (this)*/ {
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
    	/*synchronized (this)*/ {
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
    	/*synchronized (this)*/ {
	        for (Queue<E> groupQueue : taskGroups.values()) {
	            E element = groupQueue.peek();
	            if (element != null) {
	                return element;
	            }
	        }
	        throw new NoSuchElementException();
    	}
    }
}