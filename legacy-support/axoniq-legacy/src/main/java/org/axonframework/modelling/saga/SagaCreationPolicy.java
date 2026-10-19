/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga;

/**
 * Enumeration containing the possible Creation Policies for Sagas.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public enum SagaCreationPolicy {

    /**
     * Never create a new Saga instance, even if none exists.
     */
    NONE,

    /**
     * Only create a new Saga instance if none can be found.
     */
    IF_NONE_FOUND,

    /**
     * Always create a new Saga, even if one already exists.
     */
    ALWAYS
}
