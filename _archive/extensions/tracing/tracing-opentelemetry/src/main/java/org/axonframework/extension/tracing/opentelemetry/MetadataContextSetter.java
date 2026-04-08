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

package org.axonframework.extension.tracing.opentelemetry;

import io.opentelemetry.context.propagation.TextMapSetter;
import org.axonframework.messaging.core.Message;

import java.util.Map;
import org.axonframework.messaging.core.Metadata;
import org.jspecify.annotations.Nullable;

/**
 * This {@link TextMapSetter} implementation is able to insert the current OpenTelemetry span context into a
 * {@link Message}. However, since a {@code Message} is immutable, this injector injects it into the provided
 * {@link Map}. It's the responsibility the implementing {@link OpenTelemetrySpanFactory} to mutate the message through
 * {@link OpenTelemetrySpanFactory#propagateContext(Message)}.
 * <p>
 * The trace becomes the message's parent span in its{@link Metadata}.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class MetadataContextSetter implements TextMapSetter<Map<String, String>> {

    /**
     * Singleton instance of the {@link MetadataContextSetter}, used by the {@link OpenTelemetrySpanFactory}.
     */
    public static final MetadataContextSetter INSTANCE = new MetadataContextSetter();

    private MetadataContextSetter() {
        // Should not be initialized directly, use the public static INSTANCE.
    }

    @Override
    public void set(@Nullable Map<String, String> metadata, String key, String value) {
        if (metadata == null) {
            throw new IllegalArgumentException("The provided metadata may not be null!");
        }
        metadata.put(key, value);
    }
}
