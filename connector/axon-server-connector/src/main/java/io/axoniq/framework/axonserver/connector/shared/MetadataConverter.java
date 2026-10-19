/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.shared;

import io.axoniq.axonserver.grpc.MetaDataValue;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;


import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to convert between Axon Server metadata values and a simple {@link Map} of String key-value pairs.
 * <p>
 * This class provides methods to convert a {@link Map} of String key-value pairs to a {@link Map} of Axon Server
 * {@link MetaDataValue} objects and vice versa.
 *
 * @author Steven van Beelen
 * @author Mitchell Herrijgers
 * @author Allard Buijze
 * @since 5.0.0
 */
@Internal
public final class MetadataConverter {

    private MetadataConverter() {
        // Utility class
    }

    /**
     * Converts a {@link Map} of String key-value pairs to a {@link Map} of Axon Server {@link MetaDataValue} objects.
     *
     * @param source The source map containing String key-value pairs.
     * @return A map where each value is converted to an Axon Server {@link MetaDataValue}.
     */
    public static Map<String, MetaDataValue> convertGrpcToMetadataValues(Map<String, @Nullable String> source) {
        Map<String, MetaDataValue> result = new HashMap<>();
        source.forEach((k, v) -> {
            if (v != null) {
                MetaDataValue convertedValue = convertToTextMetaDataValue(v);
                result.put(k, convertedValue);
            }
        });
        return result;
    }

    private static MetaDataValue convertToTextMetaDataValue(String value) {
        return MetaDataValue.newBuilder().setTextValue(value).build();
    }

    /**
     * Converts a {@link Map} of Axon Server {@link MetaDataValue} objects to a {@link Map} of String key-value pairs.
     *
     * @param source The source map containing Axon Server {@link MetaDataValue} objects.
     * @return A map where each value is converted to a String representation.
     */
    public static Map<String, String> convertMetadataValuesToGrpc(Map<String, MetaDataValue> source) {
        Map<String, String> result = new HashMap<>();
        source.forEach((k, v) -> {
            String convertedValue = convertFromMetaDataValue(v);
            if (convertedValue != null) {
                result.put(k, convertedValue);
            }
        });
        return result;
    }

    @Nullable
    private static String convertFromMetaDataValue(MetaDataValue value) {
        return switch (value.getDataCase()) {
            case TEXT_VALUE -> value.getTextValue();
            case DOUBLE_VALUE -> Double.toString(value.getDoubleValue());
            case NUMBER_VALUE -> Long.toString(value.getNumberValue());
            case BOOLEAN_VALUE -> Boolean.toString(value.getBooleanValue());
            default -> null;
        };
    }
}