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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.dsl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import jakarta.annotation.Nonnull;
import org.axonframework.conversion.Converter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Payload manipulation helper.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class Payload {

    public static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };
    private final Map<String, Object> payload;

    public Payload(@Nonnull Map<String, Object> payload) {
        this.payload = Objects.requireNonNull(payload, "Payload must not be null");
    }

    public static Payload payload(
            @Nonnull WorkflowContext context,
            @Nonnull Object value
    ) {
        var converter = Objects.requireNonNull(context, "Workflow context must not be null")
                               .processingContext().component(Converter.class);
        Map<String, Object> map = Objects.requireNonNull(converter.convert(value, PAYLOAD_TYPE.getType()),
                                                         "Payload converted to null");
        return payload(map);
    }

    public static Payload payload(@Nonnull Map<String, Object> payload) {
        return new Payload(Objects.requireNonNull(payload, "Payload must not be null"));
    }

    public static Payload payload() {
        return payload(new LinkedHashMap<>());
    }

    public static Payload payload(@Nonnull WorkflowContext context) {
        return payload(Objects.requireNonNull(context, "Workflow context must not be null").workflowPayload());
    }

    public static Payload payload(@Nonnull String key, Object value) {
        return payload().with(Objects.requireNonNull(key, "Payload key must not be null"), value);
    }

    public static Payload payload(@Nonnull String key, Object value, @Nonnull String key2, Object value2) {
        return payload(key, value)
                .with(key2, value2);
    }

    public static Payload payload(@Nonnull String key, Object value,
                                  @Nonnull String key2, Object value2,
                                  @Nonnull String key3, Object value3) {
        return payload(key, value, key2, value2).with(key3, value3);
    }

    public <T> T get(@Nonnull String key) {
        //noinspection unchecked
        return (T) payload.get(Objects.requireNonNull(key, "Payload key must not be null"));
    }

    public <T> T getPayloadAs(String key, Function<Map<String, Object>, T> converter) {
        Object value = payload.get(key);
        if (value instanceof Map) {
            //noinspection unchecked
            return (T) converter.apply((Map<String, Object>) value);
        }
        return (T) value;
    }


    public Payload set(@Nonnull String key, Object value) {
        payload.put(Objects.requireNonNull(key, "Payload key must not be null"), value);
        return this;
    }

    public Payload with(@Nonnull Payload other) {
        payload.putAll(Objects.requireNonNull(other, "Payload must not be null").getValues());
        return this;
    }

    public Payload with(@Nonnull String key, Object value) {
        return set(key, value);
    }

    public Map<String, Object> getValues() {
        return payload;
    }
}
