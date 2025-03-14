package com.example.kafkacache.kcache;

import jakarta.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * A thread-safe kafka storage like cache, data will be cached by partitions
 */
public class PartitionCache<T> {

    private final PartitionCacheConfiguration configuration;

    // hold for all partitions, key: partitionId, value: partition data
    private final ConcurrentHashMap<Integer, Partition<T>> storage = new ConcurrentHashMap<>();

    // locks for partitions, key: partitionI, value: lock
    private final ConcurrentHashMap<Integer, ReentrantLock> partitionLocks = new ConcurrentHashMap<>();

    public PartitionCache(PartitionCacheConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * Get current cache size by partitions
     *
     * @return cache size
     */
    public Map<Integer, Integer> getSize() {
        return this.storage.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().dataStorage.size()
                ));
    }

    /**
     * Add data into cache
     *
     * @param partition partition id
     * @param offset    offset number
     * @param data      raw data
     */
    public void add(int partition, long offset, T data) {
        var lock = this.partitionLocks.computeIfAbsent(partition, k -> new ReentrantLock());
        lock.lock();

        try {
            final var partitionData = this.storage.computeIfAbsent(partition, k -> new Partition<>());
            partitionData.dataStorage.put(offset, new CachedData<>(data, System.currentTimeMillis()));

            if (this.needCleanup(partitionData)) {
                this.performCleanup(partitionData);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * Get start offset for the given partition
     *
     * @param partition partition id
     * @return start offset
     */
    public Long getStartOffset(int partition) {
        final var partitionData = this.storage.get(partition);
        if (partitionData == null) {
            return 0L;
        }
        return partitionData.dataStorage.firstKey();
    }

    /**
     * Get end offset for the given partition
     *
     * @param partition partition id
     * @return end offset
     */
    public Long getEndOffset(int partition) {
        final var partitionData = this.storage.get(partition);
        if (partitionData == null) {
            return Long.MAX_VALUE;
        }
        return partitionData.dataStorage.lastKey();
    }

    /**
     * Get partition data by a given offset range
     *
     * @param partition   partition id
     * @param startOffset start offset (including), can be null (from earliest)
     * @param endOffset   end offset (including), can be null (to latest)
     * @return the range data sorted by offset
     */
    public List<T> getRange(int partition, @Nullable Long startOffset, @Nullable Long endOffset) {
        final var partitionData = this.storage.get(partition);
        if (partitionData == null) {
            return Collections.emptyList();
        }

        final var rangeData = partitionData.dataStorage.subMap(startOffset, true, endOffset, true);
        return rangeData.values().stream()
                .map(CachedData::getPayload)
                .toList();
    }

    private boolean needCleanup(Partition<T> partition) {
        return partition.dataStorage.size() > this.configuration.getMaxEntriesPerPartition() ||
                (System.currentTimeMillis() - partition.lastCleanTime) > this.configuration.getCleanupIntervalMillionSeconds();
    }

    private void performCleanup(Partition<T> partition) {
        final long now = System.currentTimeMillis();

        // clan by time window size
        final long oldestAllowed = now - this.configuration.getMaxRetentionMillionSeconds();
        partition.dataStorage.entrySet().removeIf(entry -> entry.getValue().getTimestamp() < oldestAllowed);

        // clean by entry number
        if (partition.dataStorage.size() > this.configuration.getMaxEntriesPerPartition()) {
            while (partition.dataStorage.size() > this.configuration.getMinEntriesPerPartition()) {
                partition.dataStorage.pollFirstEntry();
            }
        }

        partition.lastCleanTime = now;
    }


    // a cache store for one partition
    private static class Partition<T> {

        // a sorted map storage, key: kafka offset, value: cached data
        private final ConcurrentNavigableMap<Long, CachedData<T>> dataStorage = new ConcurrentSkipListMap<>();

        private volatile long lastCleanTime = System.currentTimeMillis();
    }
}
