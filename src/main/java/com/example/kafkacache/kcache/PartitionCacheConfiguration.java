package com.example.kafkacache.kcache;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PartitionCacheConfiguration {

    private long maxRetentionMillionSeconds;
    private long cleanupIntervalMillionSeconds;
    private int maxEntriesPerPartition;
    private int minEntriesPerPartition;
}
