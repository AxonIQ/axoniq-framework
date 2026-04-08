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

package org.axonframework.modelling.command.inspection;

import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.annotation.CommandHandlingMember;

/**
 * Interface describing a message handler capable of forwarding a specific command.
 *
 * @param <T> The type of entity to which the message handler will delegate the actual handling of the command
 * @author Somrak Monpengpinij
 * @since 4.6.0
 */
public interface ForwardingCommandHandlingMember<T> extends CommandHandlingMember<T> {

    /**
     * Check if this handler is in a state where it can currently accept the command.
     *
     * @param message The message that is to be forwarded.
     * @param target  The target to forward the command message.
     * @return {@code true} if this handler can forward command to target entity, {@code false} otherwise.
     */
    boolean canForward(CommandMessage message, T target);
}
