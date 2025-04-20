package com.example.kafkacache.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroMapConvertorTest {

    @InjectMocks
    private AvroMapConvertor convertor;

    private Schema testSchema;

    private AutoCloseable disp;

    @BeforeEach
    void setUp() {
        disp = MockitoAnnotations.openMocks(this);
        testSchema = createTestSchema();
    }

    @AfterEach
    void clear() throws Exception {
        disp.close();
    }

    @Test
    void shouldConvertBasicTypesToGenericRecord() {
        Map<String, Object> input = Map.of(
                "name", "John",
                "age", 30,
                "score", 95.5
        );

        GenericRecord record = convertor.toGenericRecord(input, testSchema);

        assertThat(record.get("name").toString()).isEqualTo("John");
        assertThat(record.get("age")).isEqualTo(30);
        assertThat(record.get("score")).isEqualTo(95.5);
    }

    @Test
    void shouldHandleNestedRecords() {
        Map<String, Object> address = Map.of(
                "street", "Main St",
                "number", 123
        );
        Map<String, Object> input = Map.of(
                "name", "Alice",
                "age", "21",
                "address", address
        );

        GenericRecord record = convertor.toGenericRecord(input, testSchema);
        GenericRecord nestedRecord = (GenericRecord) record.get("address");

        assertThat(nestedRecord.get("street").toString()).isEqualTo("Main St");
        assertThat(nestedRecord.get("number")).isEqualTo(123);
    }

    @Test
    void shouldConvertCollectionTypes() {
        Map<String, Object> input = Map.of(
                "name", "Bob",
                "age", "32",
                "scores", Arrays.asList(80, 90, 100),
                "metadata", Map.of("key1", "value1", "key2", 2)
        );

        GenericRecord record = convertor.toGenericRecord(input, testSchema);

        // validate array
        var scores = (GenericData.Array<Integer>) record.get("scores");
        assertThat(scores.get(0)).isEqualTo(80);
        assertThat(scores.get(1)).isEqualTo(90);
        assertThat(scores.get(2)).isEqualTo(100);

        // validate map
        Map<?, ?> metadata = (Map<?, ?>) record.get("metadata");
        assertThat(metadata.get(new Utf8("key1")).toString()).isEqualTo("value1");
        assertThat(metadata.get(new Utf8("key2"))).isEqualTo(2);
    }

    @Test
    void shouldThrowTypeConversionException() {
        Map<String, Object> invalidInput = Map.of(
                "name", "Bob",
                "age", "32",
                "score", "wrong score"
        );

        assertThatThrownBy(() -> convertor.toGenericRecord(invalidInput, testSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unable to find a matched type for");
    }

    @Test
    void shouldThrowClassCastException() {
        Map<String, Object> invalidInput = Map.of(
                "name", "Bob",
                "age", "wrong age"
        );

        assertThatThrownBy(() -> convertor.toGenericRecord(invalidInput, testSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Type conversion failed for value");
    }

    @Test
    void shouldMaintainConversionIntegrity() {
        Map<String, Object> original = Map.of(
                "name", "Bob",
                "age", 25,
                "scores", List.of(70, 85),
                "metadata", Map.of("department", "IT")
        );

        GenericRecord record = convertor.toGenericRecord(original, testSchema);
        Map<String, Object> result = convertor.toMap(record);

        assertThat(result)
                .usingRecursiveComparison()
                .ignoringExpectedNullFields()
                .isEqualTo(original);
    }

    @Test
    void shouldReadWriteAvroFile(@TempDir Path tempDir) throws Exception {
        List<GenericRecord> records = List.of(
                convertor.toGenericRecord(Map.of(
                        "name", "Bob",
                        "age", 25,
                        "scores", List.of(70, 85),
                        "metadata", Map.of("department", "IT")
                ), testSchema)
        );

        final var path = tempDir.resolve("test.avro").toString();
        convertor.saveAvroRecords(testSchema, records, new FileOutputStream(path));

        List<GenericRecord> resultRecords = convertor.readAvroRecords(new File(path));

        assertThat(resultRecords.toString()).isEqualTo(records.toString());
    }

    private Schema createTestSchema() {
        String schemaJson = """
                {
                    "type": "record",
                    "name": "User",
                    "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "age", "type": "int"},
                        {"name": "score", "type": ["double", "null"]},
                        {"name": "address", "type": ["null",
                            {
                                "type": "record",
                                "name": "Address",
                                "fields": [
                                    {"name": "street", "type": "string"},
                                    {"name": "number", "type": "int"}
                                ]
                            }]},
                        {"name": "scores", "type": ["null", {"type": "array", "items": "int"}]},
                        {"name": "metadata", "type": ["null", {"type": "map", "values": ["string", "int"]}]}
                    ]
                }
                """;
        return new Schema.Parser().parse(schemaJson);
    }
}