package com.example.kafkacache.kcache;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class PartitionCacheTest {

    @Test
    void should_store_and_retrieve_data() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(0, 101L, new PositionView("B"));
        kCache.add(0, 102L, new PositionView("C"));

        List<PositionView> result1 = kCache.getRange(0, 100L, 102L);
        List<PositionView> result2 = kCache.getRange(0, null, null);

        assertThat(result1)
                .extracting(PositionView::getKey)
                .containsExactly("A", "B", "C");
        assertThat(result2)
                .extracting(PositionView::getKey)
                .containsExactly("A", "B", "C");
    }

    @Test
    void should_store_and_retrieve_data_from_begin() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(0, 101L, new PositionView("B"));
        kCache.add(0, 102L, new PositionView("C"));

        List<PositionView> result = kCache.getRange(0, null, 101L);

        assertThat(result)
                .extracting(PositionView::getKey)
                .containsExactly("A", "B");
    }

    @Test
    void should_store_and_retrieve_data_to_end() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(0, 101L, new PositionView("B"));
        kCache.add(0, 102L, new PositionView("C"));

        List<PositionView> result = kCache.getRange(0, 101L, null);

        assertThat(result)
                .extracting(PositionView::getKey)
                .containsExactly("B", "C");
    }

    @Test
    void should_return_empty_for_unknown_partition() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        List<PositionView> result = kCache.getRange(999, 0L, 100L);
        assertThat(result).isEmpty();
    }

    @Test
    void should_handle_empty_range() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));

        List<PositionView> result = kCache.getRange(0, 200L, 300L);

        assertThat(result).isEmpty();
    }

    @Test
    void should_handle_duplicate_offsets() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(0, 100L, new PositionView("B"));

        List<PositionView> result = kCache.getRange(0, 100L, 100L);
        assertThat(result)
                .extracting(PositionView::getKey)
                .containsExactly("B");
    }

    @Test
    void should_evict_old_entries_by_count() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(0, 101L, new PositionView("B"));
        kCache.add(0, 102L, new PositionView("C"));
        kCache.add(0, 103L, new PositionView("D"));
        kCache.add(0, 104L, new PositionView("E"));

        List<PositionView> result1 = kCache.getRange(0, null, null);
        assertThat(result1)
                .extracting(PositionView::getKey)
                .containsExactly("A", "B", "C", "D", "E");

        // will trigger clean up
        kCache.add(0, 105L, new PositionView("F"));

        List<PositionView> result2 = kCache.getRange(0, null, null);
        assertThat(result2)
                .extracting(PositionView::getKey)
                .containsExactly("D", "E", "F");
    }

    //@Test
    void should_evict_old_entries_by_time() {
        try (var mock = Mockito.mockStatic(System.class)) {
            mock.when(System::currentTimeMillis).thenReturn(0);

            final var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
            kCache.add(0, 100L, new PositionView("A"));
            mock.when(System::currentTimeMillis).thenReturn(10 * 60 * 1000);

            List<PositionView> result = kCache.getRange(0, null, null);
            assertThat(result)
                    .extracting(PositionView::getKey)
                    .containsExactly("A");

            kCache.add(0, 101L, new PositionView("B"));
            mock.when(System::currentTimeMillis).thenReturn(30 * 60 * 1000);

            assertThat(result)
                    .extracting(PositionView::getKey)
                    .containsExactly("A", "B");

            // will trigger clean up
            kCache.add(0, 102L, new PositionView("C"));

            assertThat(result)
                    .extracting(PositionView::getKey)
                    .containsExactly("B", "C");
        }
    }

    @Test
    void should_handle_concurrent_writes() throws InterruptedException {
        final int THREADS = 10;
        final int PER_THREAD = 100;

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        AtomicInteger counter = new AtomicInteger();

        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());

        Runnable task = () -> {
            for (int i = 0; i < PER_THREAD; i++) {
                int offset = counter.incrementAndGet();
                kCache.add(0, offset, new PositionView("Data-" + offset));
            }
        };

        for (int i = 0; i < THREADS; i++) {
            executor.submit(task);
        }

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        List<PositionView> result = kCache.getRange(0, 1L, (long) THREADS * PER_THREAD);
        assertThat(result).hasSizeBetween(3, 5); // min entries ~ max entries

        var resultSize = result.size();
        var expected = IntStream.range(THREADS * PER_THREAD - resultSize + 1, THREADS * PER_THREAD + 1)
                .mapToObj(i -> String.format("Data-%d", i))
                .toList()
                .toArray(new String[resultSize]);

        assertThat(result)
                .extracting(PositionView::getKey)
                .containsExactly(expected);
    }

    @Test
    void should_handle_concurrent_reads_and_writes() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(0, 101L, new PositionView("B"));

        // concurrency: 1 write + several read
        ExecutorService executor = Executors.newFixedThreadPool(4);
        executor.submit(() -> {
            kCache.add(0, 102L, new PositionView("C"));
            kCache.add(0, 103L, new PositionView("D"));
        });

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            List<PositionView> result = kCache.getRange(0, 100L, 103L);
            assertThat(result)
                    .hasSizeBetween(3, 5)
                    .allMatch(d -> d.getKey().matches("[ABCD]"));
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    void should_clean_expired_data_via_scheduler() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(0, 101L, new PositionView("B"));
        kCache.add(0, 102L, new PositionView("C"));
        kCache.add(0, 103L, new PositionView("D"));

        var store = (ConcurrentHashMap<Integer, ?>) ReflectionTestUtils.getField(kCache, "storage");
        var partitionStore = (ConcurrentNavigableMap<Long, CachedData<PositionView>>) ReflectionTestUtils.getField(store.get(0), "dataStorage");

        // mock last cleanup expired
        ReflectionTestUtils.setField(store.get(0), "lastCleanTime", 100L);

        // mock data A expired
        partitionStore.remove(100L);
        partitionStore.put(100L, new CachedData<>(new PositionView("A"), 100L));

        List<PositionView> result1 = kCache.getRange(0, null, null);
        assertThat(result1)
                .extracting(PositionView::getKey)
                .containsExactly("A", "B", "C", "D");

        ReflectionTestUtils.invokeMethod(kCache, "scheduledCleanup");

        List<PositionView> result2 = kCache.getRange(0, null, null);
        assertThat(result2)
                .extracting(PositionView::getKey)
                .containsExactly("B", "C", "D");
    }

    @Test
    public void getSize_MultiplePartitions_ReturnsCorrectSizes() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(1, 101L, new PositionView("B"));
        kCache.add(1, 102L, new PositionView("C"));

        Map<Integer, Integer> result = kCache.getSize();
        assertThat(result).containsExactly(Map.entry(0, 1), Map.entry(1, 2));
    }

    @Test
    public void getSize_PartitionDataSizeChanges_ReturnsUpdatedSizes() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(0, 101L, new PositionView("B"));

        Map<Integer, Integer> result = kCache.getSize();
        assertThat(result).containsExactly(Map.entry(0, 2));

        kCache.add(0, 102L, new PositionView("C"));
        result = kCache.getSize();
        assertThat(result).containsExactly(Map.entry(0, 3));
    }

    @Test
    public void getStartOffset_PartitionExists_ReturnsFirstKey() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(0, 101L, new PositionView("B"));

        Long startOffset = kCache.getStartOffset(0);
        assertThat(startOffset).isEqualTo(100L);
    }

    @Test
    public void getStartOffset_PartitionDoesNotExist_ReturnsZero() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        Long startOffset = kCache.getStartOffset(0);
        assertThat(startOffset).isEqualTo(0L);
    }

    @Test
    public void getEndOffset_PartitionExists_ReturnsLastKey() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(0, 101L, new PositionView("B"));

        Long endOffset = kCache.getEndOffset(0);
        assertThat(endOffset).isEqualTo(101L);
    }

    @Test
    public void getEndOffset_PartitionDoesNotExist_ReturnsMaxValue() {
        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());
        Long endOffset = kCache.getEndOffset(0);
        assertThat(endOffset).isEqualTo(Long.MAX_VALUE);
    }


    private PartitionCacheConfiguration buildDefaultConfig() {
        var config = new PartitionCacheConfiguration();
        config.setCleanupIntervalMillionSeconds(5 * 60 * 1000);
        config.setMaxRetentionMillionSeconds(30 * 60 * 1000);
        config.setMinEntriesPerPartition(3);
        config.setMaxEntriesPerPartition(5);
        return config;
    }

    static class PositionView {

        private final String key;

        public String getKey() {
            return this.key;
        }

        public PositionView(String key) {
            this.key = key;
        }
    }

    static class TestClock {
        private final AtomicLong currentTime = new AtomicLong(System.currentTimeMillis());

        void advance(long millis) {
            this.currentTime.addAndGet(millis);
        }

        long currentTimeMillis() {
            return currentTime.get();
        }
    }
}
