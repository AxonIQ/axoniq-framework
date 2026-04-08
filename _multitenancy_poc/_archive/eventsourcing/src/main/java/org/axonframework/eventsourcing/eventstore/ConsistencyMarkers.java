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


import java.util.Objects;

/**
 * Utility class to access "well known" consistency markers, such as "origin" and "infinity".
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
abstract class ConsistencyMarkers {

    ConsistencyMarkers() {
    }

    static class OriginConsistencyMarker implements ConsistencyMarker {

        static final OriginConsistencyMarker INSTANCE = new OriginConsistencyMarker();

        private OriginConsistencyMarker() {
        }

        @Override
        public ConsistencyMarker lowerBound(ConsistencyMarker other) {
            return this;
        }

        @Override
        public ConsistencyMarker upperBound(ConsistencyMarker other) {
            return Objects.requireNonNull(other, "The other consistency marker cannot be null.");
        }

        @Override
        public Position position() {
            return Position.START;
        }

        @Override
        public String toString() {
            return "ORIGIN";
        }
    }

    static class InfinityConsistencyMarker implements ConsistencyMarker {

        static final InfinityConsistencyMarker INSTANCE = new InfinityConsistencyMarker();

        private InfinityConsistencyMarker() {
        }

        @Override
        public ConsistencyMarker lowerBound(ConsistencyMarker other) {
            return Objects.requireNonNull(other, "The consistency marker cannot be null.");
        }

        @Override
        public ConsistencyMarker upperBound(ConsistencyMarker other) {
            return this;
        }

        @Override
        public Position position() {
            throw new UnsupportedOperationException("Not yet implemented");  // not implemented because there are no use cases
        }

        @Override
        public String toString() {
            return "INFINITY";
        }
    }
}
