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

package org.axonframework.eventsourcing.handler;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.concurrent.CompletableFuture;

/**
 * Handles the sourcing of an entity from its historical event stream.
 * <p>
 * Implementations of this interface are responsible for retrieving all relevant events for a given
 * identifier and applying them to construct or evolve the entity to its current state.
 *
 * @param <I> the type of the entity identifier
 * @param <E> the type of the entity
 * @since 5.1.0
 * @author John Hendrikx
 */
@Internal
public interface SourcingHandler<I, E> extends DescribableComponent {

    /**
     * Sources the entity identified by the given {@code identifier}.
     * <p>
     * The {@link InitializingEntityEvolver} is used to either create the entity (if it does not exist)
     * or evolve it through the events retrieved from the underlying event stream.
     * <p>
     * This method returns a {@link CompletableFuture} that completes when the entity has been fully
     * reconstructed or evolved to its latest state.
     *
     * @param identifier the identifier of the entity to source, cannot be {@code null}
     * @param evolver the {@link InitializingEntityEvolver} used to initialize and evolve the entity, cannot be {@code null}
     * @param processingContext the {@link ProcessingContext} associated with this sourcing operation, cannot be {@code null}
     * @return a {@link CompletableFuture} that completes with the sourced entity, never {@code null}
     */
    CompletableFuture<E> source(I identifier, InitializingEntityEvolver<I, E> evolver, ProcessingContext processingContext);
}
