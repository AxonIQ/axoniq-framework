/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.axonserver.connector.shared;

import io.axoniq.axonserver.grpc.MetaDataValue;
import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link MetadataConverter}.
 *
 * @author Jens Mayer
 */
class MetadataConverterTest {

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
        Map<String, String> result = MetadataConverter.convertMetadataValuesToGrpc(source);

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
        Map<String, String> result = MetadataConverter.convertMetadataValuesToGrpc(source);

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
}