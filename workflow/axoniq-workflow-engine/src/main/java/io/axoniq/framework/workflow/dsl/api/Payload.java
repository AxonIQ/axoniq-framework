/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.dsl.api;

import com.fasterxml.jackson.core.type.TypeReference;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Payload manipulation helper.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class Payload {

    /**
     * Map type reference for easy access.
     */
    public static final TypeReference<Map<String, @Nullable Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };
    private final Map<String, @Nullable Object> payload;

    /**
     * Constructs a new payload around the given map.
     *
     * @param payload payload map.
     */
    public Payload(Map<String, @Nullable Object> payload) {
        this.payload = new HashMap<>(Objects.requireNonNull(payload, "Payload must not be null"));
    }

    /**
     * Constructs a new payload around the given value, converting it using converter from workflow context.
     *
     * @param context workflow context.
     * @param value   payload value.
     * @return payload representation of the given value.
     */
    public static Payload payload(
            WorkflowContext context,
            Object value
    ) {
        var converter = Objects.requireNonNull(context, "Workflow context must not be null")
                               .processingContext().component(EventConverter.class);
        Map<String, @Nullable Object> map = Objects.requireNonNull(converter.convert(value, PAYLOAD_TYPE.getType()),
                                                                   "Payload converted to null");
        return payload(map);
    }

    /**
     * Constructs a new payload around the given value.
     *
     * @param payload payload value.
     * @return payload representation of the given value.
     */
    public static Payload payload(Map<String, @Nullable Object> payload) {
        return new Payload(Objects.requireNonNull(payload, "Payload must not be null"));
    }

    /**
     * Constructs a new empty payload.
     *
     * @return empty payload.
     */
    public static Payload payload() {
        return payload(new LinkedHashMap<>());
    }

    /**
     * Constructs a new payload from workflow context.
     *
     * @param context workflow context.
     * @return payload representation of the workflow context.
     */
    public static Payload payload(WorkflowContext context) {
        return payload(Objects.requireNonNull(context, "Workflow context must not be null").workflowPayload());
    }

    /**
     * Constructs a new payload with a single key-value pair.
     *
     * @param key   property name.
     * @param value value
     * @return payload with oen value.
     */
    public static Payload payload(String key, @Nullable Object value) {
        return payload().with(Objects.requireNonNull(key, "Payload key must not be null"), value);
    }

    /**
     * Constructs a new payload with two key-value pairs.
     *
     * @param key    property name for the first value.
     * @param value  first value.
     * @param key2   property key for the second value.
     * @param value2 second value.
     * @return payload with two values.
     */
    public static Payload payload(String key, @Nullable Object value,
                                  String key2, @Nullable Object value2) {
        return payload(key, value)
                .with(key2, value2);
    }

    /**
     * Constructs a new payload with three key-value pairs.
     *
     * @param key    property name for the first value.
     * @param value  first value.
     * @param key2   property key for the second value.
     * @param value2 second value.
     * @param key3   property key for the third value.
     * @param value3 third value.
     * @return payload with three values.
     */
    public static Payload payload(String key, @Nullable Object value,
                                  String key2, @Nullable Object value2,
                                  String key3, @Nullable Object value3) {
        return payload(key, value, key2, value2).with(key3, value3);
    }

    /**
     * Retrieves a value from the payload by key.
     *
     * @param key property key to retrieve.
     * @param <T> type of the value to retrieve.
     * @return value associated with the key.
     */
    @Nullable
    public <T> T get(String key) {
        //noinspection unchecked
        return (T) payload.get(Objects.requireNonNull(key, "Payload key must not be null"));
    }

    /**
     * Retrieves a value from the payload by key and converts it using the given converter.
     *
     * @param key       property name.
     * @param converter converter to use.
     * @param <T>       type of return value.
     * @return converter payload.
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public <T> T getPayloadAs(String key, Function<Map<String, @Nullable Object>, T> converter) {
        Object value = payload.get(key);
        if (value instanceof Map) {
            //noinspection unchecked
            return (T) converter.apply((Map<String, @Nullable Object>) value);
        }
        return (T) value;
    }


    /**
     * Sets a value in the payload.
     *
     * @param key   property key.
     * @param value value to set.
     * @return payload with updated value.
     */
    public Payload set(String key, @Nullable Object value) {
        payload.put(Objects.requireNonNull(key, "Payload key must not be null"), value);
        return this;
    }

    /**
     * Merges another payload into this payload.
     *
     * @param other payload to merge.
     * @return payload with merged values.
     */
    public Payload with(Payload other) {
        payload.putAll(Objects.requireNonNull(other, "Payload must not be null").getValues());
        return this;
    }

    /**
     * Fluent payload builder.
     *
     * @param key   property key.
     * @param value value.
     * @return payload with updated value.
     */
    public Payload with(String key, @Nullable Object value) {
        return set(key, value);
    }

    /**
     * Retrieves the underlying payload map.
     *
     * @return payload map.
     */
    public Map<String, @Nullable Object> getValues() {
        return payload;
    }
}
