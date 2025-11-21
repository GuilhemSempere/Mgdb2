package fr.cirad.tools;

import java.util.concurrent.*;

import org.apache.log4j.Logger;

import java.util.Map;

public class ExpiringHashMap<K, V> {
    private final Map<K, V> map = new ConcurrentHashMap<>();
    private final Map<K, Long> timestamps = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleaner = Executors.newScheduledThreadPool(1);
    private final long expireAfterMillis;
    
    private static final Logger LOG = Logger.getLogger(ExpiringHashMap.class);
    
    public ExpiringHashMap(long expireAfterMillis) {
        this.expireAfterMillis = expireAfterMillis;
        
        // Schedule cleanup task to run every minute
        cleaner.scheduleAtFixedRate(this::cleanup, 1, 1, TimeUnit.MINUTES);
    }
    
    public V put(K key, V value) {
        timestamps.put(key, System.currentTimeMillis());
        return map.put(key, value);
    }
    
    public V get(K key) {
        Long timestamp = timestamps.get(key);
        if (timestamp != null && isExpired(timestamp)) {
            remove(key);
            return null;
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
        ExpiringHashMap<String, String> map = new ExpiringHashMap<>(3000); // 3 seconds
        
        map.put("key1", "value1");
        map.put("key2", "value2");
        
        System.out.println("Size: " + map.size());
        System.out.println("Getting key1: " + map.get("key1"));
        
        Thread.sleep(4000); // Wait 4 seconds
        
        System.out.println("Size after expiry: " + map.size());
        System.out.println("Getting key1: " + map.get("key1"));
        
        map.shutdown();
    }
}