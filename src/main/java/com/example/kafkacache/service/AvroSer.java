package com.example.kafkacache.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.idl.IdlFile;
import org.apache.avro.idl.IdlReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
public class AvroSer {

    public static Schema buildSchema() {
        try (InputStream avdlStream = AvroSer.class.getClassLoader().getResourceAsStream("avro/position.avdl")) {
            if (avdlStream == null) {
                throw new FileNotFoundException("Avro IDL文件未找到: position.avdl");
            }

            IdlFile idlFile = new IdlReader().parse(avdlStream);
            return idlFile.getMainSchema();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void saveAvro(Schema schema, GenericRecord record, String fileName) {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
            writer.create(schema, new File(fileName));
            writer.append(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static GenericRecord convertMapToGenericRecord(Map<String, Object> dataMap, Schema schema) {
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Object value = dataMap.get(fieldName);
            recordBuilder.set(fieldName, convertToAvro(value, field.schema()));
        }
        return recordBuilder.build();
    }

    private static Object convertToAvro(Object value, Schema schema) {
        final boolean isNullable = schema.isNullable();
        if (value == null) {
            if (!isNullable) {
                throw new IllegalArgumentException("Null value for non-nullable schema: " + schema);
            }
            return null;
        }

        // process union type
        if (schema.getType() == Schema.Type.UNION) {
            return handleUnion(value, schema);
        }

        // auto type converting
        try {
            return switch (schema.getType()) {
                case LONG -> convertNumeric(value, Long.class);
                case INT -> convertNumeric(value, Integer.class);
                case FLOAT -> convertNumeric(value, Float.class);
                case DOUBLE -> convertNumeric(value, Double.class);
                case STRING -> new Utf8(value.toString());
                case RECORD -> convertMapToGenericRecord((Map<String, Object>) value, schema);
                case ARRAY -> convertListToArray((List<?>) value, schema.getElementType());
                case MAP -> convertMapToAvroMap((Map<?, ?>) value, schema.getValueType());
                case BYTES -> convertToByteBuffer(value);
                default -> value;
            };
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(
                    String.format("Type conversion failed for value: %s to schema: %s", value, schema), e);
        }
    }

    private static Object handleUnion(Object value, Schema unionSchema) {
       /* List<Schema> candidateSchemas = unionSchema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .toList();*/

        List<Schema> candidateSchemas = unionSchema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .sorted((a, b) -> {
                    // 排序逻辑：数值类型优先级高于字符串
                    if (isNumericType(a) && !isNumericType(b)) return -1;
                    if (!isNumericType(a) && isNumericType(b)) return 1;
                    return 0;
                })
                .toList();

        // try candidate types by priority
        for (Schema s : candidateSchemas) {
            try {
                return convertToAvro(value, s);
            } catch (Exception ignored) {
                log.error("ignored exact matching, schema type: {}, java type: {}", s.getType(), value.getClass());
            }
        }

        throw new IllegalArgumentException("无法匹配联合类型: " + value + "，Schema: " + unionSchema);
    }

    private static GenericArray<?> convertListToArray(List<?> list, Schema elementSchema) {
        GenericData.Array<Object> array = new GenericData.Array<>(list.size(), Schema.createArray(elementSchema));
        for (Object item : list) {
            array.add(convertToAvro(item, elementSchema));
        }
        return array;
    }

    private static Map<Utf8, Object> convertMapToAvroMap(Map<?, ?> map, Schema valueSchema) {
        Map<Utf8, Object> avroMap = new HashMap<>();
        map.forEach((k, v) -> avroMap.put(new Utf8(k.toString()), convertToAvro(v, valueSchema)));
        return avroMap;
    }

    private static ByteBuffer convertToByteBuffer(Object value) {
        if (value instanceof byte[] bytes) {
            return ByteBuffer.wrap(bytes);
        } else if (value instanceof ByteBuffer byteBuffer) {
            return byteBuffer;
        } else {
            throw new IllegalArgumentException("Cannot convert to ByteBuffer: " + value.getClass());
        }
    }


    private static boolean isNumericType(Schema schema) {
        return switch (schema.getType()) {
            case INT, LONG, FLOAT, DOUBLE -> true;
            default -> false;
        };
    }

    private static <T extends Number> T convertNumeric(Object value, Class<T> targetType) {
        if (value instanceof Number number) {
            return switch (targetType.getSimpleName()) {
                case "Long" -> targetType.cast(number.longValue());
                case "Integer" -> targetType.cast(number.intValue());
                case "Float" -> targetType.cast(number.floatValue());
                case "Double" -> targetType.cast(number.doubleValue());
                default -> throw new IllegalArgumentException("Invalid java number type: " + targetType);
            };
        }

        if (value instanceof String str) {
            if (isNaNStr(str)) {
                log.error("Invalid number value: {}, return null value as the result.", str);
                return null;
            }
            return switch (targetType.getSimpleName()) {
                case "Long" -> targetType.cast(Long.parseLong(str));
                case "Integer" -> targetType.cast(Integer.parseInt(str));
                case "Float" -> targetType.cast(Float.parseFloat(str));
                case "Double" -> targetType.cast(Double.parseDouble(str));
                default -> throw new IllegalArgumentException("Invalid java number type: " + targetType);
            };
        }

        throw new ClassCastException(String.format("Cannot convert value: %s to type: %s", value, targetType));
    }

    private static boolean isNaNStr(String value) {
        if (StringUtils.isBlank(value)) {
            return true;
        }
        final String tmpStr = value.trim().toLowerCase();
        return tmpStr.equals("nan") || tmpStr.equals("null");
    }
}
