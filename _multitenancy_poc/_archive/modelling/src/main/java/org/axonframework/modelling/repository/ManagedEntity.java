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

package org.axonframework.modelling.repository;

import java.util.function.UnaryOperator;

/**
 * A wrapper around an entity whose lifecycle is being managed by an {@link Repository}.
 *
 * @param <ID> The type of identifier of the entity.
 * @param <E>  The type of the entity.
 * @author Allard Buijze
 * @since 5.0.0
 */
public interface ManagedEntity<ID, E> {

    /**
     * The identifier of the entity.
     *
     * @return The identifier of the entity.
     */
    ID identifier();

    /**
     * The current state of the entity.
     *
     * @return The current state of the entity.
     */
    E entity();

    /**
     * Change the current state of the entity using the given {code change} function.
     *
     * @param change The function applying the requested change.
     * @return The state of the entity after the change.
     */
    E applyStateChange(UnaryOperator<E> change);
}
