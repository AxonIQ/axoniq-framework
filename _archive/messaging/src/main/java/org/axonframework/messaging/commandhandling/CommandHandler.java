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

package org.axonframework.messaging.commandhandling;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageHandler;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Interface describing a handler of {@link CommandMessage commands}.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@FunctionalInterface
public interface CommandHandler extends MessageHandler {

    /**
     * Handles the given {@code command} within the given {@code context}.
     * <p>
     * The {@link CommandResultMessage result message} in the returned {@link MessageStream stream} may be {@code null}.
     * Only a {@link MessageStream#just(Message) single} or {@link MessageStream#empty() empty} result message should
     * ever be expected.
     *
     * @param command The command to handle.
     * @param context The context to the given {@code command} is handled in.
     * @return A {@code MessagesStream.Single} of a {@link CommandResultMessage}.
     */
    MessageStream.Single<CommandResultMessage> handle(CommandMessage command,
                                                      ProcessingContext context);
}
