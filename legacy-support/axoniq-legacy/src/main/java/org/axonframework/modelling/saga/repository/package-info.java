/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */


/**
 * The {@link org.axonframework.modelling.saga.repository.SagaStore} abstraction and the decorators over it.
 * <p>
 * A {@code SagaStore} finds, loads, stores and deletes saga instances together with their association values. Backing
 * implementations live in the {@code inmemory}, {@code jpa} and {@code jdbc} sub-packages;
 * {@link org.axonframework.modelling.saga.repository.CachingSagaStore} decorates any of them with a cache for saga
 * instances and for association lookups.
 * <p>
 * These types retain the synchronous Axon Framework 4 storage behavior and resource-provider model. A store takes no
 * part in transaction management: it obtains its resource from the provider it was given, so its writes commit with
 * whatever else the surrounding transaction covers.
 */
@NullMarked
package org.axonframework.modelling.saga.repository;

import org.jspecify.annotations.NullMarked;
