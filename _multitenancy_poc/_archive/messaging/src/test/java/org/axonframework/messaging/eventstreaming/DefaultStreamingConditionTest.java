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
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link DefaultStreamingCondition}.
 *
 * @author Steven van Beelen
 */
class DefaultStreamingConditionTest {

    private static final GlobalSequenceTrackingToken TEST_POSITION = new GlobalSequenceTrackingToken(1337);
    private static final EventCriteria TEST_CRITERIA = EventCriteria.havingTags("key", "value");

    private DefaultStreamingCondition testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new DefaultStreamingCondition(TEST_POSITION, TEST_CRITERIA);
    }

    @Test
    void throwsExceptionWhenConstructingWithNullPosition() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new DefaultStreamingCondition(null, TEST_CRITERIA));
    }

    @Test
    void throwsExceptionWhenConstructingWithNullCriteria() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new DefaultStreamingCondition(TEST_POSITION, null));
    }

    @Test
    void containsExpectedData() {
        assertEquals(TEST_POSITION, testSubject.position());
        assertEquals(Set.of(TEST_CRITERIA), testSubject.criteria().flatten());
    }

    @Test
    void withCriteriaCombinesGivenWithExistingCriteria() {
        EventCriteria testCriteria = EventCriteria.havingTags(new org.axonframework.messaging.eventstreaming.Tag("other-key", "other-value"))
                                                  .andBeingOneOfTypes("test-type");

        StreamingCondition result = testSubject.or(testCriteria);

        assertEquals(TEST_POSITION, result.position());
        assertTrue(result.matches(new QualifiedName("test-type"), Set.of(new org.axonframework.messaging.eventstreaming.Tag("other-key", "other-value"))));
        assertFalse(result.matches(new QualifiedName("random-type"), Set.of(new org.axonframework.messaging.eventstreaming.Tag("other-key", "other-value"))));
        assertTrue(result.matches(new QualifiedName("random-type"), Set.of(new Tag("key", "value"))));
    }
}