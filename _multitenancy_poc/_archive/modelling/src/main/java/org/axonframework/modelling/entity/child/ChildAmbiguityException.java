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

package org.axonframework.modelling.entity.child;

import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Exception indicating that multiple child entities of a parent entity are able to handle the same command. This
 * happens if multiple {@link EntityChildMetamodel#supportedCommands()} contain the same
 * {@link QualifiedName}, as well as both child entities returning true for
 * {@link EntityChildMetamodel#canHandle(CommandMessage, Object, ProcessingContext)}, indicating that they have an active
 * child entity that can handle the command.
 * <p>
 * When this happens, make sure the {@link CommandTargetResolver} is configured correctly to resolve the child entity
 * that should handle the command.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class ChildAmbiguityException extends RuntimeException {

    /**
     * Initializes the {@code ChildAmbiguityException} with the given {@code message}.
     *
     * @param message The message describing the cause of this exception.
     */
    public ChildAmbiguityException(String message) {
        super(message);
    }
}
