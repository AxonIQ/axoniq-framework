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

import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link NoAppendCondition}.
 *
 * @author Steven van Beelen
 */
class NoAppendConditionTest {

    @Test
    void consistencyMarkerFixedToLongMax() {
        assertEquals(ConsistencyMarker.INFINITY, AppendCondition.none().consistencyMarker());
    }

    @Test
    void criteriaFixedToNoCriteria() {
        assertEquals(EventCriteria.havingAnyTag(), AppendCondition.none().criteria());
    }

    @Test
    void withMarkerThrowsUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () -> AppendCondition.none().withMarker(mock()));
    }
}