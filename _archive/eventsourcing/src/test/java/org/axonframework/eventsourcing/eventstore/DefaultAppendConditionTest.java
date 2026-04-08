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

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link DefaultAppendCondition}.
 *
 * @author Steven van Beelen
 */
class DefaultAppendConditionTest {

    private static final ConsistencyMarker TEST_CONSISTENCY_MARKER = new GlobalIndexConsistencyMarker(10);
    private static final EventCriteria TEST_CRITERIA = EventCriteria.havingTags("key", "value");
    private static final EventCriteria OTHER_CRITERIA = EventCriteria.havingTags("other_key", "other");

    private AppendCondition testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new DefaultAppendCondition(TEST_CONSISTENCY_MARKER, TEST_CRITERIA);
    }

    @Test
    void throwsExceptionWhenConstructingWithNullEventCriteria() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class,
                     () -> new DefaultAppendCondition(ConsistencyMarker.ORIGIN, (EventCriteria) null));
    }

    @Test
    void containsExpectedData() {
        assertEquals(TEST_CONSISTENCY_MARKER, testSubject.consistencyMarker());
        assertEquals(Set.of(TEST_CRITERIA), testSubject.criteria().flatten());
    }

    @Test
    void withMarkerChangesMarkerButLeavesConditions() {
        ConsistencyMarker testConsistencyMarker = new GlobalIndexConsistencyMarker(5);

        AppendCondition result = testSubject.withMarker(testConsistencyMarker);

        assertEquals(testConsistencyMarker, result.consistencyMarker());
        assertEquals(testSubject.criteria(), result.criteria());
    }

    @Test
    void orCriteriaAreCombinedWithExistingCriteria() {
        ConsistencyMarker testConsistencyMarker = new GlobalIndexConsistencyMarker(5);

        AppendCondition result = testSubject.withMarker(testConsistencyMarker)
                                            .orCriteria(TEST_CRITERIA)
                                            .orCriteria(OTHER_CRITERIA);
        assertTrue(result.criteria().flatten().contains(TEST_CRITERIA));
        assertTrue(result.criteria().flatten().contains(OTHER_CRITERIA));
    }
}