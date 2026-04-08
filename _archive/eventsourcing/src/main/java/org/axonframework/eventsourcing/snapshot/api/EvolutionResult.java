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

package org.axonframework.eventsourcing.snapshot.api;

import java.time.Duration;
import java.util.Objects;

/**
 * Represents metrics collected while sourcing an event-sourced entity to its current state.
 * <p>
 * {@code EvolutionResult} captures information about the sourcing process,
 * including how many events were applied, how long the process took,
 * and whether a snapshot was requested during sourcing.
 * <p>
 * This information is provided to the {@link Snapshotter} once sourcing
 * has completed, allowing it to make an informed decision about creating
 * and persisting a snapshot.
 *
 * @param eventsApplied the number of events applied while sourcing the entity,
 *                      never negative
 * @param sourcingTime the total time spent sourcing the entity, never {@code null}
 * @author John Hendrikx
 * @since 5.1.0
 */
public record EvolutionResult(long eventsApplied, Duration sourcingTime) {

    /**
     * Creates a new {@code EvolutionResult}.
     *
     * @throws NullPointerException if {@code sourcingTime} is {@code null}
     * @throws IllegalArgumentException if {@code eventsApplied} is negative
     */
    public EvolutionResult {
        Objects.requireNonNull(sourcingTime, "The sourcingTime parameter must not be null.");

        if (eventsApplied < 0) {
            throw new IllegalArgumentException("The eventsApplied parameter must be non-negative.");
        }
    }
}