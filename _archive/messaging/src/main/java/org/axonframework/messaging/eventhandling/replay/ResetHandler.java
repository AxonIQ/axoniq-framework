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

package org.axonframework.messaging.eventhandling.replay;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageHandler;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Functional interface for handling {@link ResetContext} messages.
 * <p>
 * Implementations of this interface process reset notifications, typically to prepare for
 * event replay by cleaning up the projection state, clearing caches, or performing other
 * reset-related operations.
 * <p>
 * Reset handlers are registered via {@link ResetHandlerRegistry#subscribe(ResetHandler)}.
 * <p>
 * Example usage:
 * <pre>{@code
 * ResetHandler handler = (resetContext, context) -> {
 *     repository.deleteAll();
 *     cache.clear();
 *     return MessageStream.empty();
 * };
 * }</pre>
 *
 * @author Mateusz Nowak
 * @see ResetContext
 * @see ResetHandlerRegistry
 * @see org.axonframework.messaging.eventhandling.EventHandlingComponent
 * @since 5.0.0
 */
@FunctionalInterface
public interface ResetHandler extends MessageHandler {

    /**
     * Handles the given {@link ResetContext} message, performing any necessary reset operations.
     * <p>
     * This method is invoked before the processor begins replaying events. Handlers typically
     * use this opportunity to clean up state that will be rebuilt during replay.
     * <p>
     * If this method completes exceptionally, the reset operation will be aborted and no
     * replay will occur.
     *
     * @param resetContext The reset context message containing reset information and optional payload.
     * @param context The processing context for this operation.
     * @return An empty message stream after handling completes successfully.
     */
    MessageStream.Empty<Message> handle(ResetContext resetContext,
                                        ProcessingContext context);
}
