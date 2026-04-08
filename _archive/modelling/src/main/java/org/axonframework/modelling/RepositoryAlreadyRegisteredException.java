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
 * Exception thrown by the {@link StateManager} when trying to register an {@link Repository} for a combination of
 * entity type and id type for which a repository was already registered. Super- or subtypes are considered a match of
 * each other, so the repository can unambiguously resolve an {@link Repository} for a given combination of entity type
 * and id type and prevent runtime errors.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class RepositoryAlreadyRegisteredException extends RuntimeException {

    /**
     * Initialize the exception with a message that contains the types of the conflicting repositories.
     *
     * @param repository         The repository that was attempted to be registered.
     * @param existingRepository The repository that was already registered for the conflicting entity type.
     */
    public RepositoryAlreadyRegisteredException(Repository<?, ?> repository,
                                                Repository<?, ?> existingRepository) {
        super("Cannot register repository for state type [%s] with id type [%s] as conflicting repository for entity type [%s] with [%s] type was already registered.".formatted(
                repository.entityType().getName(),
                repository.idType().getName(),
                existingRepository.entityType().getName(),
                existingRepository.idType().getName()
        ));
    }
}
