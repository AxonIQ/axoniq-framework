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
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * Adds the aggregate identifier to the span when one is available on the {@link ProcessingContext} via
 * {@link LegacyResources#AGGREGATE_IDENTIFIER_KEY}. By default the attribute key is {@link #AGGREGATE_IDENTIFIER}
 * ({@code axoniq.aggregate.identifier}); a different key can be supplied through the constructor — for example to keep
 * the Axon Framework 4 key {@code axon_aggregate_identifier}.
 * <p>
 * This is best-effort: the attribute is present only when a legacy aggregate-based event storage engine populated the
 * resource. It is absent for dynamic-consistency-boundary / entity-based operations and whenever no context is
 * available. This provider intentionally does not reference any Axon Framework 4-era message type.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
public final class AggregateIdentifierSpanAttributesProvider implements SpanAttributesProvider {

    /**
     * Default attribute key under which the aggregate identifier is recorded.
     */
    public static final String AGGREGATE_IDENTIFIER = "axoniq.aggregate.identifier";

    private final String attributeKey;

    /**
     * Creates a provider recording the aggregate identifier under the default {@link #AGGREGATE_IDENTIFIER} key.
     */
    public AggregateIdentifierSpanAttributesProvider() {
        this(AGGREGATE_IDENTIFIER);
    }

    /**
     * Creates a provider recording the aggregate identifier under the given {@code attributeKey}.
     *
     * @param attributeKey the span attribute key to record the aggregate identifier under
     */
    public AggregateIdentifierSpanAttributesProvider(String attributeKey) {
        this.attributeKey = Objects.requireNonNull(attributeKey, "attributeKey may not be null");
    }

    @Override
    public Map<String, String> provideForMessage(Message message, @Nullable ProcessingContext context) {
        if (context == null) {
            return Map.of();
        }
        String aggregateIdentifier = context.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY);
        if (aggregateIdentifier == null) {
            return Map.of();
        }
        return Map.of(attributeKey, aggregateIdentifier);
    }
}
