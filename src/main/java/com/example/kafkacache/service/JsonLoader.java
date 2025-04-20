package com.example.kafkacache.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class JsonLoader {
    public static Map<String, Object> readJsonToMap(String fileName) {
        ObjectMapper objectMapper = new ObjectMapper();
        try (InputStream inputStream = JsonLoader.class.getClassLoader().getResourceAsStream("sample/" + fileName)) {
            if (inputStream == null) {
                throw new RuntimeException("File not found: " + fileName);
            }
            return objectMapper.readValue(inputStream, new TypeReference<>() {
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
