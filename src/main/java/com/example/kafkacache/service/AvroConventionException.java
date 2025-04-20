package com.example.kafkacache.service;

public class AvroConventionException extends RuntimeException {

    public AvroConventionException(String message) {
        super(message);
    }

    public AvroConventionException(String message, Throwable cause) {
        super(message, cause);
    }
}
