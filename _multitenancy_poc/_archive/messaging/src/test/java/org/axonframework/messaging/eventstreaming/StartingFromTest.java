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

package org.axonframework.messaging.eventstreaming;

import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link StartingFrom}.
 *
 * @author Steven van Beelen
 */
class StartingFromTest {

    private static final GlobalSequenceTrackingToken TEST_POSITION = new GlobalSequenceTrackingToken(1337);
    private static final EventCriteria TEST_CRITERIA = EventCriteria.havingTags("key", "value");

    private StreamingCondition testSubject;

    @BeforeEach
    void setUp() {
        testSubject = StreamingCondition.startingFrom(TEST_POSITION);
    }

    @Test
    void containsExpectedData() {
        assertEquals(TEST_POSITION, testSubject.position());
        assertEquals(EventCriteria.havingAnyTag(), testSubject.criteria());
    }

    @Test
    void withCriteriaReplaceNoCriteriaForGivenCriteria() {
        assertEquals(EventCriteria.havingAnyTag(), testSubject.criteria());

        StreamingCondition result = testSubject.or(TEST_CRITERIA);

        assertEquals(TEST_CRITERIA, result.criteria());
    }

    @Test
    void withCriteriaThrowsIllegalArgumentExceptionWhenPositionIsNull() {
        StreamingCondition nullPositionTestSubject = StreamingCondition.startingFrom(null);

        assertThrows(IllegalArgumentException.class, () -> nullPositionTestSubject.or(TEST_CRITERIA));
    }
}