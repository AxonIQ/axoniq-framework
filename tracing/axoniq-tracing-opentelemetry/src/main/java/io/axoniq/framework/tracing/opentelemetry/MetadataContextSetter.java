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

import io.opentelemetry.context.propagation.TextMapSetter;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * {@link TextMapSetter} implementation that writes the currently-active OpenTelemetry trace context into a mutable
 * {@link Map}.
 * <p>
 * Since an {@link org.axonframework.messaging.core.Message} is immutable, this setter cannot mutate the message
 * directly. Instead it writes the propagation entries into a temporary {@code Map}, which the
 * {@link OpenTelemetrySpanFactory} then merges onto the message via
 * {@link org.axonframework.messaging.core.Message#andMetadata(Map)}. The resulting trace context becomes the parent
 * span recorded in the message's {@link org.axonframework.messaging.core.Metadata}.
 * <p>
 * This type is {@link Internal} because it is an implementation detail of the OpenTelemetry binding; it is exposed
 * only so the {@link OpenTelemetrySpanFactory} and tests can reference the shared {@link #INSTANCE}.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@Internal
public final class MetadataContextSetter implements TextMapSetter<Map<String, String>> {

    /**
     * Shared singleton instance, used by the {@link OpenTelemetrySpanFactory}.
     */
    public static final MetadataContextSetter INSTANCE = new MetadataContextSetter();

    private MetadataContextSetter() {
        // Should not be initialized directly, use the public static INSTANCE.
    }

    @Override
    public void set(@Nullable Map<String, String> carrier, String key, String value) {
        if (carrier == null) {
            throw new IllegalArgumentException("The provided carrier may not be null!");
        }
        carrier.put(key, value);
    }
}
