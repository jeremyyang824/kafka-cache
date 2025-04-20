package com.example.kafkacache;

import com.example.kafkacache.service.Runner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaCacheApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(KafkaCacheApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        new Runner().run();
    }
}
