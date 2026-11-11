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
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.Converter;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    private static final Logger logger = LoggerFactory.getLogger(MetadataConverter.class);

    private static final int MAX_LOGGED_KEY_COUNT = 1_000;
    private static final Set<String> loggedNoneConvertedKeys = ConcurrentHashMap.newKeySet();

    private MetadataConverter() {
        // Utility class
    }

    /**
     * Converts a {@link Map} of String key-value pairs to a {@link Map} of Axon Server {@link MetaDataValue} objects.
     *
     * @param source The source map containing String key-value pairs.
     * @return A map where each value is converted to an Axon Server {@link MetaDataValue}.
     */
    public static Map<String, MetaDataValue> convertMetadataMapToGrpc(Map<String, @Nullable String> source) {
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
     * <p>
     * Text, double, number and boolean values are converted to their {@code String} representation. A
     * {@code bytes_value}, which Axon Framework 4 used for anything that was not text, a number or a boolean, is
     * rendered as a {@code String} through the given {@code converter}. When the {@code converter} cannot do so, or is
     * {@code null}, the bytes are used as UTF-8 text, such as JSON or XML. When these bytes are not valid text, for
     * example due to a binary serializer, the key is kept with a {@code null} value and a warning is logged <b>once</b>
     * per key. A {@link MetaDataValue} without any value is converted to a {@code null} value as well, without a
     * warning.
     *
     * @param source    the source map containing Axon Server {@link MetaDataValue} objects
     * @param converter the converter used to render {@code bytes_value} entries as a {@code String}, if any
     * @return a map where each value is converted to a String representation, or {@code null} if it cannot be converted
     */
    public static Map<String, @Nullable String> convertGrpcToMetadataMap(Map<String, MetaDataValue> source,
                                                                         @Nullable Converter converter) {
        Map<String, @Nullable String> result = new HashMap<>();
        source.forEach((k, v) -> {
            String convertedValue = convertFromMetaDataValue(v, converter);
            if (convertedValue == null && v.getDataCase() != MetaDataValue.DataCase.DATA_NOT_SET
                    && firstKeyEncounter(k)) {
                logger.warn("Metadata entry with key [{}] has a value of type [{}] that cannot be converted to a "
                                    + "String and will be [null].",
                            k, v.getDataCase());
            }
            result.put(k, convertedValue);
        });
        return result;
    }

    private static boolean firstKeyEncounter(String key) {
        // Warn once per key to prevent flooding the log when replaying many messages with the same unconvertible key.
        // The tracked keys are capped, so arbitrary keys cannot grow this set indefinitely.
        return loggedNoneConvertedKeys.size() < MAX_LOGGED_KEY_COUNT && loggedNoneConvertedKeys.add(key);
    }

    @Nullable
    private static String convertFromMetaDataValue(MetaDataValue value, @Nullable Converter converter) {
        return switch (value.getDataCase()) {
            case TEXT_VALUE -> value.getTextValue();
            case DOUBLE_VALUE -> Double.toString(value.getDoubleValue());
            case NUMBER_VALUE -> Long.toString(value.getNumberValue());
            case BOOLEAN_VALUE -> Boolean.toString(value.getBooleanValue());
            case BYTES_VALUE -> renderBytes(value.getBytesValue().getData(), converter);
            default -> null;
        };
    }

    @Nullable
    private static String renderBytes(ByteString data, @Nullable Converter converter) {
        String text = decodeUtf8(data);
        if (text == null || converter == null) {
            return text;
        }
        try {
            String rendered = converter.convert(data.toByteArray(), String.class);
            return rendered != null ? rendered : text;
        } catch (ConversionException e) {
            logger.debug("Converter could not render serialized metadata value as a String. Using its text instead.",
                         e);
            return text;
        }
    }

    @Nullable
    private static String decodeUtf8(ByteString data) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                                         .onMalformedInput(CodingErrorAction.REPORT)
                                         .onUnmappableCharacter(CodingErrorAction.REPORT)
                                         .decode(data.asReadOnlyByteBuffer())
                                         .toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }
}
