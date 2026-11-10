/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.shared;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link MetadataConverter}.
 *
 * @author Jens Mayer
 */
class MetadataConverterTest {

    private final Converter converter = new JacksonConverter();

    @Test
    void convertsEmptyMap() {
        // Given
        Map<String, String> source = new HashMap<>();

        // When
        Map<String, MetaDataValue> result = MetadataConverter.convertGrpcToMetadataValues(source);

        // Then
        assertThat(result)
                .isNotNull()
                .isEmpty();
    }

    @Test
    void convertsMapWithMultipleEntriesFromGrpcToMetaData() {
        // Given
        Map<String, String> source = Map.of(
                "key1", "value1",
                "key2", "value2",
                "key3", "value3"
        );

        // When
        Map<String, MetaDataValue> result = MetadataConverter.convertGrpcToMetadataValues(source);

        // Then
        assertThat(result)
                .isNotNull()
                .hasSize(3);

        source.forEach((key, value) -> {
            assertThat(result).containsKey(key);
            MetaDataValue metaDataValue = result.get(key);
            assertThat(metaDataValue.getTextValue()).isEqualTo(value);
            assertThat(metaDataValue.getDataCase()).isEqualTo(MetaDataValue.DataCase.TEXT_VALUE);
        });
    }

    @Test
    void convertsEmptyMetaDataMapToGrpc() {
        // Given
        Map<String, MetaDataValue> source = new HashMap<>();

        // When
        Map<String, String> result = MetadataConverter.convertMetadataValuesToGrpc(source, converter);

        // Then
        assertThat(result)
                .isNotNull()
                .isEmpty();
    }

    @Test
    void convertMetaDataValuesToAppropriateGrpcTypes() {
        var textKey = "textKey";
        var textValue = "textValue";
        var doubleKey = "doubleKey";
        var doubleValue = 1.0;
        var numberKey = "numberKey";
        var numberValue = 1L;
        var booleanKey = "booleanKey";
        var booleanValue = true;

        // Given
        Map<String, MetaDataValue> source = Map.of(
                textKey, MetaDataValue.newBuilder().setTextValue(textValue).build(),
                doubleKey, MetaDataValue.newBuilder().setDoubleValue(doubleValue).build(),
                numberKey, MetaDataValue.newBuilder().setNumberValue(numberValue).build(),
                booleanKey, MetaDataValue.newBuilder().setBooleanValue(booleanValue).build()
        );

        // When
        Map<String, String> result = MetadataConverter.convertMetadataValuesToGrpc(source, converter);

        // Then
        assertThat(result)
                .isNotNull()
                .hasSize(4);
        assertThat(result)
                .containsEntry(textKey, textValue)
                .containsEntry(doubleKey,String.valueOf(doubleValue))
                .containsEntry(numberKey,String.valueOf(numberValue))
                .containsEntry(booleanKey, String.valueOf(booleanValue));
    }

    @Test
    void rendersBytesValueEntriesThroughConverterWhenConvertingToMetadata() {
        // given
        Map<String, MetaDataValue> source = Map.of(
                "mapKey", bytesValue("{\"key\":\"v\"}".getBytes(StandardCharsets.UTF_8)),
                "listKey", bytesValue("[1,2]".getBytes(StandardCharsets.UTF_8))
        );

        // when
        Map<String, String> result = MetadataConverter.convertMetadataValuesToGrpc(source, converter);

        // then
        assertThat(result)
                .hasSize(2)
                .containsEntry("mapKey", "{\"key\":\"v\"}")
                .containsEntry("listKey", "[1,2]");
    }

    @Test
    void decodesBytesValueEntriesAsTextWithoutConverterWhenConvertingToMetadata() {
        // given
        Map<String, MetaDataValue> source = Map.of(
                "xmlKey", bytesValue("<map><entry>v</entry></map>".getBytes(StandardCharsets.UTF_8))
        );

        // when
        Map<String, String> result = MetadataConverter.convertMetadataValuesToGrpc(source, null);

        // then
        assertThat(result).containsOnly(Map.entry("xmlKey", "<map><entry>v</entry></map>"));
    }

    @Test
    void decodesBytesValueEntriesAsTextWhenConverterCannotReadThemWhenConvertingToMetadata() {
        // given XML, which a Jackson converter cannot read
        Map<String, MetaDataValue> source = Map.of(
                "xmlKey", bytesValue("<map><entry>v</entry></map>".getBytes(StandardCharsets.UTF_8))
        );

        // when
        Map<String, String> result = MetadataConverter.convertMetadataValuesToGrpc(source, converter);

        // then
        assertThat(result).containsOnly(Map.entry("xmlKey", "<map><entry>v</entry></map>"));
    }

    @Test
    void convertsNonTextBytesValueEntriesToNullWhenConvertingToMetadata() {
        // given
        Map<String, MetaDataValue> source = Map.of(
                "textKey", MetaDataValue.newBuilder().setTextValue("textValue").build(),
                "binaryKey", bytesValue(new byte[]{(byte) 0xAC, (byte) 0xED, (byte) 0xFF, (byte) 0xFE})
        );

        // when
        Map<String, String> result = MetadataConverter.convertMetadataValuesToGrpc(source, converter);

        // then
        assertThat(result)
                .hasSize(2)
                .containsEntry("textKey", "textValue")
                .containsEntry("binaryKey", null);
    }

    private static MetaDataValue bytesValue(byte[] data) {
        return MetaDataValue.newBuilder().setBytesValue(
                SerializedObject.newBuilder().setType("java.util.Map").setData(ByteString.copyFrom(data)).build()
        ).build();
    }

    @Test
    void convertsEntriesWithoutValueToNullWhenConvertingToMetadata() {
        // Given
        Map<String, MetaDataValue> source = Map.of("emptyKey", MetaDataValue.getDefaultInstance());

        // When
        Map<String, String> result = MetadataConverter.convertMetadataValuesToGrpc(source, converter);

        // Then
        assertThat(result).hasSize(1).containsEntry("emptyKey", null);
    }
}
