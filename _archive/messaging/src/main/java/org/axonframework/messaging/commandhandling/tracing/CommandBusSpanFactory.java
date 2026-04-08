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

package org.axonframework.messaging.commandhandling.tracing;

import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.tracing.Span;

/**
 * Span factory that creates spans for the {@link CommandBus}. You can customize the spans of the bus by creating your
 * own implementation.
 *
 * @author Mitchell Herrijgers
 * @since 4.9.0
 */
public interface CommandBusSpanFactory {

    /**
     * Creates a span for the dispatching of a command.
     *
     * @param commandMessage The command message to create a span for.
     * @param distributed    Whether the command is distributed or not.
     * @return The created span.
     */
    Span createDispatchCommandSpan(CommandMessage commandMessage, boolean distributed);

    /**
     * Creates a span for the handling of a command.
     *
     * @param commandMessage The command message to create a span for.
     * @param distributed    Whether the command is distributed or not.
     * @return The created span.
     */
    Span createHandleCommandSpan(CommandMessage commandMessage, boolean distributed);

    /**
     * Propagates the context of the current span to the given command message.
     *
     * @param commandMessage The command message to propagate the context to.
     * @param <T>            The type of the payload of the command message.
     * @return The command message with the propagated context.
     */
    <T> CommandMessage propagateContext(CommandMessage commandMessage);
}
