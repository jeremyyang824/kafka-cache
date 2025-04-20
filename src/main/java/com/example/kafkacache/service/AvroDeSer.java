package com.example.kafkacache.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class AvroDeSer {

    public static GenericRecord readAvro(String fileName) {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        final File avroFile = new File(fileName);
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(avroFile, datumReader)) {
            if (reader.hasNext()) {
                return reader.next();
            }
            return null;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static Map<String, Object> convertGenericRecordToMap(GenericRecord record) {
        Map<String, Object> result = new HashMap<>();
        Schema schema = record.getSchema();
        for (Schema.Field field : schema.getFields()) {
            Object value = record.get(field.name());
            if (value == null) {
                continue;
            }
            result.put(field.name(), convertFromAvro(value, field.schema()));
        }
        return result;
    }

    private static Object convertFromAvro(Object avroValue, Schema schema) {
        if (avroValue == null) {
            return null;
        }

        // process union type
        if (schema.getType() == Schema.Type.UNION) {
            return handleUnionFromAvro(avroValue, schema);
        }

        // process basic types
        return switch (schema.getType()) {
            case RECORD -> convertGenericRecordToMap((GenericRecord) avroValue);
            case ARRAY -> convertAvroArray((GenericArray<?>) avroValue, schema);
            case MAP -> convertAvroMap((Map<Utf8, Object>) avroValue, schema);
            case LONG -> convertNumeric(avroValue, Long.class);
            case INT -> convertNumeric(avroValue, Integer.class);
            case FLOAT -> convertNumeric(avroValue, Float.class);
            case DOUBLE -> convertNumeric(avroValue, Double.class);
            case STRING -> avroValue.toString();
            case BYTES -> convertByteBuffer((ByteBuffer) avroValue);
            default -> avroValue;
        };
    }

    private static Object handleUnionFromAvro(Object avroValue, Schema unionSchema) {
        for (Schema memberSchema : unionSchema.getTypes()) {
            if (memberSchema.getType() == Schema.Type.NULL) {
                continue;
            }

            try {
                return convertFromAvro(avroValue, memberSchema);
            } catch (Exception e) {
                // ignore exception and try next type
            }
        }
        throw new IllegalArgumentException(
                String.format("Cannot convert avro value: %s, schema: %s.", avroValue, unionSchema));
    }

    private static <T extends Number> T convertNumeric(Object avroValue, Class<T> targetType) {
        Number number = (Number) avroValue;
        return switch (targetType.getSimpleName()) {
            case "Long" -> targetType.cast(number.longValue());
            case "Integer" -> targetType.cast(number.intValue());
            case "Float" -> targetType.cast(number.floatValue());
            case "Double" -> targetType.cast(number.doubleValue());
            default -> throw new IllegalArgumentException("Invalid java number type: " + targetType);
        };
    }

    private static List<Object> convertAvroArray(GenericArray<?> array, Schema schema) {
        List<Object> list = new ArrayList<>();
        Schema elementSchema = schema.getElementType();
        for (Object element : array) {
            list.add(convertFromAvro(element, elementSchema));
        }
        return list;
    }

    private static Map<String, Object> convertAvroMap(Map<Utf8, Object> avroMap, Schema schema) {
        Map<String, Object> javaMap = new HashMap<>();
        Schema valueSchema = schema.getValueType();
        avroMap.forEach((k, v) ->
                javaMap.put(k.toString(), convertFromAvro(v, valueSchema))
        );
        return javaMap;
    }

    private static byte[] convertByteBuffer(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
