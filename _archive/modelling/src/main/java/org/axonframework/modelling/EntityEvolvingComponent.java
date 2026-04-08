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
import org.axonframework.messaging.core.QualifiedName;

import java.util.Set;

/**
 * Interface describing a group of {@link EntityEvolver EntityEvolvers} belonging to a single entity of type {@code E},
 * forming an entity that can evolve its state based on the events it receives.
 * <p>
 * The {@link #supportedEvents()} describes the events supported by this entity evolver.
 *
 * @param <E> The entity type to evolve.
 * @author Steven van Beelen
 * @since 5.0.0
 */
public interface EntityEvolvingComponent<E> extends EntityEvolver<E> {

    /**
     * All supported {@link EventMessage events}, referenced through a {@link QualifiedName}.
     *
     * @return All supported {@link EventMessage events}, referenced through a {@link QualifiedName}.
     */
    Set<QualifiedName> supportedEvents();
}
