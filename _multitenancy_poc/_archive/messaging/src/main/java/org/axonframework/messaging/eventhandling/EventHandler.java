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

package org.axonframework.messaging.eventhandling;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageHandler;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Interface describing a handler of {@link EventMessage events}.
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 * @since 0.1.0
 */
@FunctionalInterface
public interface EventHandler extends MessageHandler {

    /**
     * Handles the given {@code event} within the given {@code context}.
     * <p>
     * The result of handling is an {@link MessageStream.Empty empty stream}.
     *
     * @param event   The event to handle.
     * @param context The context to the given {@code event} is handled in.
     * @return An {@link MessageStream.Empty empty stream} containing nothing.
     */
    MessageStream.Empty<Message> handle(EventMessage event,
                                                 ProcessingContext context);
}
