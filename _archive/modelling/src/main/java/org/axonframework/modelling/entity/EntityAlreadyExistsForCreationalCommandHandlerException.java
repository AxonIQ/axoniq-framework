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

package org.axonframework.modelling.entity;

import org.axonframework.messaging.commandhandling.CommandMessage;

/**
 * Exception indicating that a creational command handler was invoked for an entity that already exists.
 * <p>
 * If this command is valid for instances of the entity, as well as creational commands, an instance command can be
 * defined for the same {@link CommandMessage#type()}.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class EntityAlreadyExistsForCreationalCommandHandlerException extends RuntimeException {

    /**
     * Creates a new exception with the given {@code command} and {@code existingEntity}.
     *
     * @param command        The {@link CommandMessage} that was handled.
     * @param existingEntity The existing entity that was found.
     */
    public EntityAlreadyExistsForCreationalCommandHandlerException(CommandMessage command,
                                                                   Object existingEntity) {
        super(String.format(
                "Creational command handler for command [%s] encountered an already existing entity: [%s]",
                command.type(),
                existingEntity
        ));
    }
}
