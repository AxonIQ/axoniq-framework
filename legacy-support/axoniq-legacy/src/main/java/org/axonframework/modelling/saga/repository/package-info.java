/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
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
