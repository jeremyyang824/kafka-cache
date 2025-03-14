package com.example.kafkacache.kcache;

import lombok.Getter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class PartitionCacheTest {

    @Test
    void should_store_and_retrieve_data() {


        var kCache = new PartitionCache<PositionView>(this.buildDefaultConfig());

        // 添加测试数据
        kCache.add(0, 100L, new PositionView("A"));
        kCache.add(0, 101L, new PositionView("B"));
        kCache.add(0, 102L, new PositionView("C"));

        // 查询范围
        List<PositionView> result = kCache.getRange(0, 100L, 102L);

        // 验证结果
        assertThat(result)
                .extracting(PositionView::getKey)
                .containsExactly("A", "B", "C");
    }

    private PartitionCacheConfiguration buildDefaultConfig() {
        var config = new PartitionCacheConfiguration();
        config.setCleanupIntervalMillionSeconds(5 * 60 * 1000);
        config.setMaxRetentionMillionSeconds(30 * 60 * 1000);
        config.setMinEntriesPerPartition(100_000);
        config.setMinEntriesPerPartition(150_000);
        return config;
    }

    @Getter
    static class PositionView {

        private final String key;

        public PositionView(String key) {
            this.key = key;
        }
    }
}
