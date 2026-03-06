package fr.cirad.tools;

import java.util.concurrent.*;
import org.apache.log4j.Logger;
import java.util.Map;

public class ExpiringHashMap<K, V> {

    private final Map<K, V> map = new ConcurrentHashMap<>();
    private final Map<K, Long> timestamps = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleaner = Executors.newScheduledThreadPool(1);
    private final long expireAfterMillis;
    private final boolean refreshOnAccess;

    private static final Logger LOG = Logger.getLogger(ExpiringHashMap.class);

    public ExpiringHashMap(long expireAfterMillis) {
        this(expireAfterMillis, false);
    }
    
    /**
     * @param expireAfterMillis TTL in milliseconds
     * @param refreshOnAccess if true, get() refreshes TTL (sliding), else fixed TTL
     */
    public ExpiringHashMap(long expireAfterMillis, boolean refreshOnAccess) {
        this.expireAfterMillis = expireAfterMillis;
        this.refreshOnAccess = refreshOnAccess;

        // Schedule cleanup task to run every minute
        cleaner.scheduleAtFixedRate(this::cleanup, 1, 1, TimeUnit.MINUTES);
    }

    public V put(K key, V value) {
        timestamps.put(key, System.currentTimeMillis());
        return map.put(key, value);
    }

    public V get(K key) {
        Long timestamp = timestamps.get(key);
        if (timestamp == null) {
            return null;
        }

        if (isExpired(timestamp)) {
            remove(key);
            return null;
        }

        // Sliding TTL: refresh timestamp on access
        if (refreshOnAccess) {
            timestamps.put(key, System.currentTimeMillis());
        }

        return map.get(key);
    }

    public V remove(K key) {
        timestamps.remove(key);
        return map.remove(key);
    }

    public int size() {
        cleanup(); // Clean before returning size
        return map.size();
    }

    public boolean containsKey(K key) {
        V value = get(key); // This will handle expiry check
        return value != null;
    }

    private boolean isExpired(long timestamp) {
        return System.currentTimeMillis() - timestamp > expireAfterMillis;
    }

    private void cleanup() {
        long currentTime = System.currentTimeMillis();
        timestamps.entrySet().removeIf(entry -> {
            if (currentTime - entry.getValue() > expireAfterMillis) {
                K key = entry.getKey();
                map.remove(key);
                LOG.debug("Expired and removed: " + key);
                return true;
            }
            return false;
        });
    }

    public void shutdown() {
        cleaner.shutdown();
    }

    public static void main(String[] args) throws InterruptedException {
        // Sliding TTL example
        ExpiringHashMap<String, String> slidingMap = new ExpiringHashMap<>(3000, true);

        slidingMap.put("key1", "value1");
        slidingMap.put("key2", "value2");

        System.out.println("Size: " + slidingMap.size());
        System.out.println("Getting key1: " + slidingMap.get("key1"));

        Thread.sleep(2000); // 2 seconds
        System.out.println("Getting key1 again: " + slidingMap.get("key1")); // refresh TTL

        Thread.sleep(2000); // another 2 seconds
        System.out.println("Getting key1 after 4s: " + slidingMap.get("key1")); // still alive if sliding

        // Fixed TTL example
        ExpiringHashMap<String, String> fixedMap = new ExpiringHashMap<>(3000, false);
        fixedMap.put("keyA", "valueA");

        Thread.sleep(4000);
        System.out.println("Fixed TTL, keyA: " + fixedMap.get("keyA")); // expired

        slidingMap.shutdown();
        fixedMap.shutdown();
    }
}