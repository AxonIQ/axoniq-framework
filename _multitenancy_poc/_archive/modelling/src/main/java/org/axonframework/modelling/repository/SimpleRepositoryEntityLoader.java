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

import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.concurrent.CompletableFuture;

/**
 * Functional interface describing a component capable of loading an entity with the given identifier for the
 * {@link SimpleRepository}. The entity is loaded within the given {@link ProcessingContext}.
 *
 * @param <I> The type of the identifier of the entity.
 * @param <T> The type of the entity.
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
@FunctionalInterface
public interface SimpleRepositoryEntityLoader<I, T> {

    /**
     * Load an entity with given {@code id} within the given {@code context}.
     *
     * @param id      The identifier of the entity to load.
     * @param context The context in which the entity should be loaded.
     * @return a CompletableFuture that resolves to the loaded entity.
     */
    CompletableFuture<? extends T> load(I id, ProcessingContext context);
}
