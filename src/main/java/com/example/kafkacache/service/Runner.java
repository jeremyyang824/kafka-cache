package com.example.kafkacache.service;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@Component
@Slf4j
public class Runner {

    private static final String DATA_PATH = "C:\\Projects\\kafka-cache\\src\\main\\resources\\data\\";

    private final ResourceLoader resourceLoader;

    @SneakyThrows
    public void run() {
        final var avroConvertor = new AvroMapConvertor();
        final var avdlResource = this.resourceLoader.getResource("classpath:avro/position.avdl");
        final Schema schema = avroConvertor.loadSchema(avdlResource.getInputStream());

        final var files = List.of("position1.json", "position2.json", "position3.json", "position4.json");

        var result = files.stream()
                .map(JsonLoader::readJsonToMap)
                // test BigDecimal, DateTime
                /* .map(json -> {
                     var indic = (Map<String, Object>) json.get("indicative");
                     indic.put("coupon", new BigDecimal("3.25"));
                     indic.put("eventDateTime", OffsetDateTime.now());
                     return json;
                 })*/
                .map(json -> List.of(json, avroConvertor.toGenericRecord(json, schema)))
                .map(tmp -> {
                    // save avro to file
                    var originJson = (Map<String, Object>) tmp.get(0);
                    var avro = (GenericRecord) tmp.get(1);

                    try {
                        var fileName = String.format("%s%s.avro", DATA_PATH, originJson.get("key"));
                        avroConvertor.saveAvroRecords(schema, List.of(avro), new FileOutputStream(fileName));
                        return List.of(originJson, fileName);
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(tmp -> {
                    var originJson = tmp.get(0);
                    var avroFile = (String) tmp.get(1);

                    var avro = avroConvertor.readAvroRecords(new File(avroFile));
                    var json = avroConvertor.toMap(avro.get(0));
                    return List.of(originJson, avro, json);
                })
                .toList();

        log.info("DeSer Method execution time");

    }
}
