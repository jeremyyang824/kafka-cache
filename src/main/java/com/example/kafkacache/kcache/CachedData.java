package com.example.kafkacache.kcache;

import lombok.Data;

/**
 * A cache data wrap
 *
 * @param <T> actual cached data
 */
@Data
public class CachedData<T> {

    private final T payload;
    private final long timestamp;
}
