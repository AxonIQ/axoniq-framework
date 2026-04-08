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

import java.util.List;

/**
 * Exception indicating that the {@link PolymorphicEntityMetamodel} for a given class cannot handle a command
 * because it is of the wrong type. This typically occurs when a polymorphic entity is passed to a command handler, but
 * the entity type does not match the expected type for that command.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class WrongPolymorphicEntityTypeException extends RuntimeException {

    /**
     * Constructs the exception with the given {@code commandMessage}, the {@code givenEntity} that was passed to the
     * command handler, and the list of {@code supportedEntityTypes} that would be able to handle the command.
     *
     * @param commandMessage        The {@link CommandMessage} that was handled.
     * @param polymorphicEntityType The entity type that was passed to the command handler.
     * @param supportedEntityTypes  The list of entity types that are able to handle the command.
     * @param givenEntityType       The entity type that was passed to the command handler.
     * @param <E>                   The type of the polymorphic entity.
     */
    public <E> WrongPolymorphicEntityTypeException(CommandMessage commandMessage,
                                                   Class<E> polymorphicEntityType,
                                                   List<Class<E>> supportedEntityTypes,
                                                   Class<E> givenEntityType
    ) {
        super(String.format(
                "PolymorphicEntityMetamodel [%s] can not handle command [%s] as it is of the wrong type [%s]. Expected one of the following types: [%s]",
                polymorphicEntityType.getName(),
                commandMessage.type(),
                givenEntityType.getName(),
                supportedEntityTypes.stream()
                                    .map(Class::getName)
                                    .reduce((a, b) -> a + ", " + b)
                                    .orElse("none")
        ));
    }
}
