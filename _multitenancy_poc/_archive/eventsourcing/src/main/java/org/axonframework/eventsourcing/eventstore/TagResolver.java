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

package org.axonframework.eventsourcing.eventstore;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.Set;

/**
 * Functional interface towards resolving a {@link Set} of {@link Tag Tags} for a given {@link EventMessage}.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@FunctionalInterface
public interface TagResolver {

    /**
     * Resolves a {@link Set} of {@link Tag Tags} for the given {@code event}.
     *
     * @param event The event to resolve a {@link Set} of {@link Tag Tags} for.
     * @return A {@link Set} of {@link Tag Tags} for the given {@code event}.
     */
    Set<Tag> resolve(EventMessage event);
}
