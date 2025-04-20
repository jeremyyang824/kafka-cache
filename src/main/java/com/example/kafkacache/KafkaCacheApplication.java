package com.example.kafkacache;

import com.example.kafkacache.service.Runner;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@RequiredArgsConstructor
@SpringBootApplication
public class KafkaCacheApplication implements CommandLineRunner {

    private final Runner runner;

    public static void main(String[] args) {
        SpringApplication.run(KafkaCacheApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        this.runner.run();
    }
}
