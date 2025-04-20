package com.example.kafkacache.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.idl.IdlFile;
import org.apache.avro.idl.IdlReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

@Slf4j
public class AvroMapConvertor {

    /**
     * Load Avro IDL schema from input stream
     */
    public Schema loadSchema(InputStream avdlStream) {
        Objects.requireNonNull(avdlStream, "Avro IDL input stream is required.");
        try {
            IdlFile idlFile = new IdlReader().parse(avdlStream);
            return idlFile.getMainSchema();
        } catch (IOException e) {
            throw new AvroConventionException(String.format("Parse Avro IDL failed: %s", e.getMessage()), e);
        }
    }

    /**
     * Save Avro records to output stream
     *
     * @param schema  Avro Schema
     * @param records input Avro records
     * @param output  output stream
     */
    public void saveAvroRecords(Schema schema, List<GenericRecord> records, OutputStream output) {
        Objects.requireNonNull(schema, "Schema cannot be null");
        Objects.requireNonNull(records, "Records cannot be null");
        Objects.requireNonNull(output, "Output stream cannot be null");

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
            writer.create(schema, output);
            for (GenericRecord record : records) {
                try {
                    writer.append(record);
                } catch (IOException e) {
                    log.error("Failed to append record: {}", record, e);
                }
            }
        } catch (IOException e) {
            throw new AvroConventionException(String.format("Failed to save Avro records: %s", e.getMessage()), e);
        }
    }

    @SneakyThrows
    public List<GenericRecord> readAvroRecords(File dataFile) {
        Objects.requireNonNull(dataFile, "Data file cannot be null");
        if (!dataFile.exists() || !dataFile.canRead()) {
            throw new IOException("Invalid or unreadable file: " + dataFile.getAbsolutePath());
        }

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(dataFile, datumReader)) {
            final List<GenericRecord> result = new ArrayList<>();
            while (reader.hasNext()) {
                result.add(reader.next());
            }
            return result;
        }
    }

    /**
     * Convert a Map[String, Object] object to Avro GenericRecord record
     *
     * @param dataMap input Map data
     * @param schema  Avro Schema
     * @return Avro GenericRecord record
     */
    public GenericRecord toGenericRecord(Map<String, Object> dataMap, Schema schema) {
        return MapToAvro.convertMapToGenericRecord(dataMap, schema);
    }

    /**
     * Convert an Avro GenericRecord record to Map[String, Object]
     *
     * @param record input Avro GenericRecord record
     * @return Map[String, Object] object
     */
    public Map<String, Object> toMap(GenericRecord record) {
        return AvroToMap.convertGenericRecordToMap(record);
    }

    /**
     * Convert Map[String, Object] to Avro GenericRecord record
     */
    static class MapToAvro {
        public static GenericRecord convertMapToGenericRecord(Map<String, Object> dataMap, Schema schema) {
            GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
            for (Schema.Field field : schema.getFields()) {
                String fieldName = field.name();
                Object value = dataMap.get(fieldName);
                recordBuilder.set(fieldName, convertToAvro(value, field.schema()));
            }
            return recordBuilder.build();
        }

        @SuppressWarnings("unchecked")
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
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Type conversion failed for value: %s to schema: %s", value, schema), e);
            }
        }

        private static Object handleUnion(Object value, Schema unionSchema) {

            List<Schema> candidateSchemas = unionSchema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .sorted((a, b) -> {
                        // numeric type in high priority
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

            throw new IllegalArgumentException("Unable to find a matched type for: " + value + "，schema: " + unionSchema);
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

    /**
     * Convert Avro GenericRecord record to Map[String, Object]
     */
    static class AvroToMap {
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

        @SuppressWarnings("unchecked")
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
            final List<Object> list = new ArrayList<>();
            Schema elementSchema = schema.getElementType();
            for (Object element : array) {
                list.add(convertFromAvro(element, elementSchema));
            }
            return list;
        }

        private static Map<String, Object> convertAvroMap(Map<Utf8, Object> avroMap, Schema schema) {
            final Map<String, Object> javaMap = new HashMap<>();
            Schema valueSchema = schema.getValueType();
            avroMap.forEach((k, v) ->
                    javaMap.put(k.toString(), convertFromAvro(v, valueSchema))
            );
            return javaMap;
        }

        private static byte[] convertByteBuffer(ByteBuffer buffer) {
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
    }
}
