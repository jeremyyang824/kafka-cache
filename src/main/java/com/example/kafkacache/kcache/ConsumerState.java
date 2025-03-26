package com.example.kafkacache.kcache;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerState<T> {

    // 主存储offset（线程安全）
    final ConcurrentHashMap<Integer, AtomicLong> committedOffsets = new ConcurrentHashMap<>();
    // 本地offset缓存（无锁快速访问）
    final ConcurrentHashMap<Integer, Long> localOffsets = new ConcurrentHashMap<>();
    // 待处理记录队列
    final BlockingQueue<CachedData<T>> pendingRecords = new LinkedBlockingQueue<>();
    // 批量大小配置
    final int batchSize;
    // 最后提交时间
    volatile long lastCommitTime = System.currentTimeMillis();

    ConsumerState(int batchSize) {
        this.batchSize = batchSize;
    }
}