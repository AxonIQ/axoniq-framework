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
 * {@link ConsistencyMarker} implementation that uses a single `long` to represent a position in an event stream.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
public class GlobalIndexConsistencyMarker extends AbstractConsistencyMarker<GlobalIndexConsistencyMarker> {

    private final long position;

    /**
     * Creates a marker for the given {@code position}.
     *
     * @param position The position in the event stream this marker represents.
     */
    public GlobalIndexConsistencyMarker(long position) {
        this.position = position;
    }

    /**
     * Utility function to resolve the position from given {@code consistencyMarker}. This implementation takes into
     * account that the given {@code consistencyMarker} may be either {@link ConsistencyMarker#ORIGIN} or
     * {@link ConsistencyMarker#INFINITY}.
     *
     * @param consistencyMarker The marker to retrieve the position from.
     * @return a long representation of the position described by the consistency marker.
     */
    public static long position(ConsistencyMarker consistencyMarker) {
        if (consistencyMarker instanceof GlobalIndexConsistencyMarker gicm) {
            return gicm.position;
        } else if (consistencyMarker == ConsistencyMarker.ORIGIN) {
            return -1;
        } else if (consistencyMarker == ConsistencyMarker.INFINITY) {
            return Long.MAX_VALUE;
        }
        throw new IllegalArgumentException(consistencyMarker + " is not a global index consistency marker");
    }

    @Override
    protected ConsistencyMarker doLowerBound(GlobalIndexConsistencyMarker other) {
        return other.position < this.position ? other : this;
    }

    @Override
    protected ConsistencyMarker doUpperBound(GlobalIndexConsistencyMarker other) {
        return other.position > this.position ? other : this;
    }

    @Override
    public Position position() {
        return new GlobalIndexPosition(position);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GlobalIndexConsistencyMarker that = (GlobalIndexConsistencyMarker) o;
        return position == that.position;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(position);
    }

    @Override
    public String toString() {
        return "GlobalIndexConsistencyMarker{" +
                "position=" + position +
                '}';
    }
}
