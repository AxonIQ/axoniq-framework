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
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Adds the high-level message category ({@code COMMAND}, {@code EVENT} or {@code QUERY}) to the span under the
 * {@code axoniq.message.type} attribute. For messages that are none of those, the message's simple class name is used.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
public final class MessageTypeSpanAttributesProvider implements SpanAttributesProvider {

    /**
     * Attribute key under which the message category is recorded.
     */
    public static final String MESSAGE_TYPE = "axoniq.message.type";

    @Override
    public Map<String, String> provideForMessage(Message message, @Nullable ProcessingContext context) {
        return Map.of(MESSAGE_TYPE, categoryOf(message));
    }

    private static String categoryOf(Message message) {
        if (message instanceof CommandMessage) {
            return "COMMAND";
        }
        if (message instanceof EventMessage) {
            return "EVENT";
        }
        if (message instanceof QueryMessage) {
            return "QUERY";
        }
        return message.getClass().getSimpleName();
    }
}
