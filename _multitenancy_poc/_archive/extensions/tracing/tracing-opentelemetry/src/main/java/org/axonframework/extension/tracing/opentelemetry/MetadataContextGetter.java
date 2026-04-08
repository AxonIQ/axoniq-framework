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

import io.opentelemetry.context.propagation.TextMapGetter;
import org.axonframework.messaging.core.Message;

import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.tracing.SpanFactory;
import org.jspecify.annotations.Nullable;

/**
 * This {@link TextMapGetter} implementation is able to extract the parent OpenTelemetry span context from a
 * {@link Message}.
 * <p>
 * The trace parent is part of the message's {@link Metadata}, if it was set when
 * dispatching by the {@link MetadataContextSetter}. This is done using the
 * {@link SpanFactory#propagateContext(Message)} method for the message.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class MetadataContextGetter implements TextMapGetter<Message> {

    /**
     * Singleton instance of the {@link MetadataContextGetter}, used by the {@link OpenTelemetrySpanFactory}.
     */
    public static final MetadataContextGetter INSTANCE = new MetadataContextGetter();

    private MetadataContextGetter() {
        // Should not be initialized directly, use the public static INSTANCE.
    }

    @Override
    public Iterable<String> keys(Message message) {
        return message.metadata().keySet();
    }

    @Override
    @Nullable
    public String get(@Nullable Message message, String key) {
        if (message == null) {
            return null;
        }
        return message.metadata().get(key);
    }
}
