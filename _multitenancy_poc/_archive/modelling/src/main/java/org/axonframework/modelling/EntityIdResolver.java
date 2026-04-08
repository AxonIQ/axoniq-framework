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

package org.axonframework.modelling;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Resolver for the id of an entity. The id is then used to load an entity from the
 * {@link org.axonframework.modelling.StateManager}.
 * <p>
 * Before version 5.0.0, this interface was known as {@code org.axonframework.modelling.command.CommandTargetResolver}.
 * This interface was changed to be able to resolve any type of entity id, not just Strings, and to have the
 * {@link ProcessingContext} available for resolving the id.
 *
 * @param <ID> The type of the identifier.
 * @author Allard Buijze
 * @author Mitchell Herrijgers
 * @see org.axonframework.modelling.StateManager
 * @since 5.0.0
 */
@FunctionalInterface
public interface EntityIdResolver<ID> {

    /**
     * Resolve the id of the entity from the given {@code message} and {@code context}.
     *
     * @param message The message to resolve the id from.
     * @param context The context in which the message is processed.
     * @return The id of the entity.
     * @throws EntityIdResolutionException When the id could not be resolved.
     */
    ID resolve(Message message, ProcessingContext context) throws EntityIdResolutionException;
}
