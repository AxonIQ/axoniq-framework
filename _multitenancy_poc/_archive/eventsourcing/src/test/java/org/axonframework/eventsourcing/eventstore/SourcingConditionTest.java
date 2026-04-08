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
 * Test class validating the {@code static} factory methods and {@code default} methods of the
 * {@link SourcingCondition}.
 *
 * @author Steven van Beelen
 */
class SourcingConditionTest {

    private static final EventCriteria TEST_CRITERIA = EventCriteria.havingTags("key", "value");
    private static final GlobalIndexPosition TEST_START = new GlobalIndexPosition(42);

    @Test
    void conditionForCriteria() {
        SourcingCondition result = SourcingCondition.conditionFor(TEST_CRITERIA);

        assertEquals(TEST_CRITERIA, result.criteria());
        assertEquals(Position.START, result.start());
    }

    @Test
    void conditionForCriteriaAndStartPosition() {
        SourcingCondition result = SourcingCondition.conditionFor(TEST_START, TEST_CRITERIA);

        assertEquals(TEST_CRITERIA, result.criteria());
        assertEquals(TEST_START, result.start());
    }
}