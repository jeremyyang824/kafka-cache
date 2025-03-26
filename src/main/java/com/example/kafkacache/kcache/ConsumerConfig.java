package com.example.kafkacache.kcache;

import lombok.Data;

import java.util.function.Function;

@Data
public class ConsumerConfig {
    private String groupId;
    private Function<Integer, Long> initialOffsetFetcher;
    private int batchSize;                  // 批量大小
}
