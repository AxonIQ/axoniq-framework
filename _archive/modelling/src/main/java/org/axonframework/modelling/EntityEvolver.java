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

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Functional interface describing how to evolve a given {@code entity} of type {@code e} based on a given
 * {@link EventMessage}.
 *
 * @param <E> The entity type to evolve.
 * @author Allard Buijze
 * @author Mateusz Nowak
 * @author Mitchell Herrijgers
 * @author Steven van Beelen
 * @since 5.0.0
 */
@FunctionalInterface
public interface EntityEvolver<E> {

    /**
     * Evolve the given {@code entity} by applying the given {@code event} to it.
     *
     * @param event   The event that might adjust the {@code entity}.
     * @param entity  The current entity to evolve with the given {@code event}.
     * @param context The context within which to evolve the {@code entity} by the given {@code event}.
     * @return The evolved {@code entity} based on the given {@code event}, or the same {@code entity} when nothing
     * happened.
     */
    E evolve(E entity,
             EventMessage event,
             ProcessingContext context);
}
