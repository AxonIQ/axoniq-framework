/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga.repository;

import org.axonframework.common.caching.Cache;
import org.axonframework.common.caching.WeakReferenceCache;

/**
 * Concrete implementation of the {@link CachingSagaStoreTest} using the {@link WeakReferenceCache}.
 *
 * @author Steven van Beelen
 */
class WeakReferenceCachingSagaStoreTest extends CachingSagaStoreTest {

    @Override
    Cache sagaCache() {
        return new WeakReferenceCache();
    }

    @Override
    Cache associationCache() {
        return new WeakReferenceCache();
    }
}
