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

package io.axoniq.framework.tracing.opentelemetry;

import io.opentelemetry.context.propagation.TextMapGetter;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * {@link TextMapGetter} implementation that extracts a propagated OpenTelemetry trace context from a message's
 * {@link org.axonframework.messaging.core.Metadata}, represented as a {@code Map<String, String>}.
 * <p>
 * The trace parent is part of the message metadata when it was set on dispatch by the {@link MetadataContextSetter}
 * (through {@link io.axoniq.framework.tracing.SpanFactory#propagateContext(org.axonframework.messaging.core.Message)}).
 * The {@link OpenTelemetrySpanFactory} uses this getter to reconstruct the remote parent when creating handler spans,
 * so cross-process parenting works.
 * <p>
 * This type is {@link Internal} because it is an implementation detail of the OpenTelemetry binding; it is exposed
 * only so the {@link OpenTelemetrySpanFactory} and tests can reference the shared {@link #INSTANCE}.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@Internal
public final class MetadataContextGetter implements TextMapGetter<Map<String, String>> {

    /**
     * Shared singleton instance, used by the {@link OpenTelemetrySpanFactory}.
     */
    public static final MetadataContextGetter INSTANCE = new MetadataContextGetter();

    private MetadataContextGetter() {
        // Should not be initialized directly, use the public static INSTANCE.
    }

    @Override
    public Iterable<String> keys(Map<String, String> carrier) {
        return carrier.keySet();
    }

    @Override
    public @Nullable String get(@Nullable Map<String, String> carrier, String key) {
        if (carrier == null) {
            return null;
        }
        return carrier.get(key);
    }
}
