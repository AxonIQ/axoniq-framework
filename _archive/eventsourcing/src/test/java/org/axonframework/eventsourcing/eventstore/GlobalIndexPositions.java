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
 * Test helper class to access package private constructor.
 */
public class GlobalIndexPositions {

    /**
     * Constructs a new {@link GlobalIndexPosition} with the given index.
     *
     * @param index an index
     * @return a new {@link GlobalIndexPosition}, never {@code null}
     */
    public static GlobalIndexPosition of(long index) {
        return new GlobalIndexPosition(index);
    }
}
