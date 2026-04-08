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

/**
 * Test class validating the {@link DefaultSourcingCondition}.
 *
 * @author Steven van Beelen
 * @author John Hendrikx
 */
class DefaultSourcingConditionTest {

    private static final EventCriteria TEST_CRITERIA = EventCriteria.havingTags("key", "value");

    @Test
    void throwsExceptionWhenConstructingWithNullEventCriteria() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class,
                     () -> new DefaultSourcingCondition(Position.START, null));
    }

    @Test
    void combineUsesTheSmallestStartValue() {
        GlobalIndexPosition position1 = new GlobalIndexPosition(10);
        GlobalIndexPosition position2 = new GlobalIndexPosition(20);
        SourcingCondition sc1 = new DefaultSourcingCondition(position1, TEST_CRITERIA);
        SourcingCondition sc2 = new DefaultSourcingCondition(position2, TEST_CRITERIA);

        SourcingCondition result1 = sc1.or(sc2);
        SourcingCondition result2 = sc2.or(sc1);

        assertNotEquals(position2, result1.start());
        assertEquals(position1, result1.start());
        assertNotEquals(position2, result2.start());
        assertEquals(position1, result2.start());
    }

    @Test
    void combineWithStandardPositionUsesTheSmallestStartValue() {
        GlobalIndexPosition position = new GlobalIndexPosition(10);
        SourcingCondition sc1 = new DefaultSourcingCondition(Position.START, TEST_CRITERIA);
        SourcingCondition sc2 = new DefaultSourcingCondition(position, TEST_CRITERIA);

        SourcingCondition result1 = sc1.or(sc2);
        SourcingCondition result2 = sc2.or(sc1);

        assertNotEquals(position, result1.start());
        assertEquals(Position.START, result1.start());
        assertNotEquals(position, result2.start());
        assertEquals(Position.START, result2.start());
    }
}