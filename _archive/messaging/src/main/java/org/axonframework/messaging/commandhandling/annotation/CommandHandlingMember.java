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

package org.axonframework.messaging.commandhandling.annotation;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;

/**
 * Interface describing a message handler capable of handling a specific command.
 *
 * @param <T> The type of entity to which the message handler will delegate the actual handling of the command.
 * @author Allard Buijze
 * @since 3.0.0
 */
@Internal
public interface CommandHandlingMember<T> extends MessageHandlingMember<T> {

    /**
     * Returns the name of the command that can be handled.
     * <p>
     * Might be an empty {@link String} when undefined by this handling member, in which case components gathering
     * {@code CommandHandlingMembers} should fall back to other mechanisms to define the name of a handling member.
     *
     * @return The name of the command that can be handled.
     */
    String commandName();

    /**
     * Returns the property of the command that is to be used as routing key towards this command handler instance. If
     * multiple handlers instances are available, a sending component is responsible to route commands with the same
     * routing key value to the correct instance.
     *
     * @return The property of the command to use as routing key.
     */
    String routingKey();

    /**
     * Check if this message handler creates a new instance of the entity of type {@code T} to handle this command.
     * <p>
     * This is for instance the case if the message is handled in the constructor method of the entity.
     *
     * @return {@code true} if this handler is also factory for entities, {@code false} otherwise.
     */
    boolean isFactoryHandler();
}
