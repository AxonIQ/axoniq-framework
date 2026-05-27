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

package io.axoniq.framework.tracing.attributes;

import io.axoniq.framework.tracing.SpanAttributesProvider;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * Adds the {@link Message#payloadType() payload type} to the span. By default the attribute key is
 * {@link #PAYLOAD_TYPE} ({@code axoniq.message.payload_type} — dotted namespace with a snake_case leaf, per the
 * OpenTelemetry attribute-naming convention); a different key can be supplied through the constructor — for example to
 * keep the Axon Framework 4 key {@code axon_payload_type}.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
public final class PayloadTypeSpanAttributesProvider implements SpanAttributesProvider {

    /**
     * Default attribute key under which the message payload type is recorded.
     */
    public static final String PAYLOAD_TYPE = "axoniq.message.payload_type";

    private final String attributeKey;

    /**
     * Creates a provider recording the payload type under the default {@link #PAYLOAD_TYPE} key.
     */
    public PayloadTypeSpanAttributesProvider() {
        this(PAYLOAD_TYPE);
    }

    /**
     * Creates a provider recording the payload type under the given {@code attributeKey}.
     *
     * @param attributeKey the span attribute key to record the payload type under
     */
    public PayloadTypeSpanAttributesProvider(String attributeKey) {
        this.attributeKey = Objects.requireNonNull(attributeKey, "attributeKey may not be null");
    }

    @Override
    public Map<String, String> provideForMessage(Message message, @Nullable ProcessingContext context) {
        return Map.of(attributeKey, message.payloadType().getName());
    }
}
