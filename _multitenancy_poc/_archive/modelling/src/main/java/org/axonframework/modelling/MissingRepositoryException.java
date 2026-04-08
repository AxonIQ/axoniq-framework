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

import org.axonframework.modelling.repository.Repository;

/**
 * Exception thrown by the {@link StateManager} when no {@link Repository} is registered for a given state type.
 * Can be resolved by registering a {@link Repository} for the given state type.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class MissingRepositoryException extends RuntimeException {

    /**
     * Initialize the exception with a message containing the given state type.
     *
     * @param entityType The state type for which no repository was registered.
     */
    public MissingRepositoryException(Class<?> idType, Class<?> entityType) {
        super("No repository was registered for the given entity type [%s] with id type [%s]".formatted(
                entityType.getName(),
                idType.getName()
        ));
    }
}
