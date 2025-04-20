package com.example.kafkacache.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
public class Runner {

    private static final String DATA_PATH = "C:\\Projects\\kafka-cache\\src\\main\\resources\\data\\";

    public void run() {
        final Schema schema = AvroSer.buildSchema();
        final var files = List.of("position1.json");

        var result = files.stream()
                .map(JsonLoader::readJsonToMap)
                // test BigDecimal, DateTime
                .map(json -> {
                    var indic = (Map<String, Object>) json.get("indicative");
                    indic.put("coupon", new BigDecimal("3.25"));
                    indic.put("eventDateTime", OffsetDateTime.now());
                    return json;
                })
                .map(json -> List.of(json, AvroSer.convertMapToGenericRecord(json, schema)))
                .map(tmp -> {
                    // save avro to file
                    var originJson = (Map<String, Object>) tmp.get(0);
                    var avro = (GenericRecord) tmp.get(1);
                    var fileName = String.format("%s%s.avro", DATA_PATH, originJson.get("key"));
                    AvroSer.saveAvro(schema, avro, fileName);
                    return List.of(originJson, fileName);
                })
                .map(tmp -> {
                    var originJson = tmp.get(0);
                    var avroFile = (String) tmp.get(1);

                    var avro = AvroDeSer.readAvro(avroFile);
                    var json = AvroDeSer.convertGenericRecordToMap(avro);
                    return List.of(originJson, avro, json);
                })
                .toList();


        log.info("DeSer Method execution time");

    }
}
