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

/**
 * Represents a position from which sourcing may start.
 *
 * @author John Hendrikx
 * @since 5.0.0
 */
public sealed interface Position permits GlobalIndexPosition, AggregateSequenceNumberPosition, StartPosition {

    /**
     * Represents the smallest possible position.
     */
    Position START = new StartPosition();

    /**
     * Returns the smallest of the two positions. If one of the positions is {@link #START},
     * this function will always return this value. Implementors must ensure this operation
     * is symmetric, and so should always return {@link #START} when called with it.
     *
     * @param other Another position, cannot be {@code null}.
     * @return The smallest of the two positions, never {@code null}.
     * @throws NullPointerException When any argument is {@code null}.
     * @throws IllegalArgumentException When the given position is incompatible with this position.
     */
    Position min(Position other);
}
