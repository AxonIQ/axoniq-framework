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

package org.axonframework.common.annotation;

import org.axonframework.common.Priority;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PriorityAnnotationComparatorTest {

    private PriorityAnnotationComparator<Object> testSubject;

    @BeforeEach
    void setUp() {
        testSubject = PriorityAnnotationComparator.getInstance();
    }

    @Test
    void compareDifferentPriorities() {
        assertTrue(testSubject.compare(new LowPrio(), new HighPrio()) > 0);
        assertTrue(testSubject.compare(new LowPrio(), new NeutralPrio()) > 0);
        assertTrue(testSubject.compare(new NeutralPrio(), new HighPrio()) > 0);

        assertTrue(testSubject.compare(new HighPrio(), new LowPrio()) < 0);
        assertTrue(testSubject.compare(new NeutralPrio(), new LowPrio()) < 0);
        assertTrue(testSubject.compare(new HighPrio(), new NeutralPrio()) < 0);
    }

    @Test
    void compareSamePriorities() {
        assertEquals(0, testSubject.compare(new LowPrio(), new LowPrio()));
        assertEquals(0, testSubject.compare(new NeutralPrio(), new NeutralPrio()));
        assertEquals(0, testSubject.compare(new HighPrio(), new HighPrio()));
    }

    @Test
    void undefinedConsideredNeutralPriority() {
        assertTrue(testSubject.compare(new UndefinedPrio(), new HighPrio()) > 0);
        assertTrue(testSubject.compare(new UndefinedPrio(), new LowPrio()) < 0);
        assertTrue(testSubject.compare(new UndefinedPrio(), new NeutralPrio()) == 0);

        assertTrue(testSubject.compare(new HighPrio(), new UndefinedPrio()) < 0);
        assertTrue(testSubject.compare(new LowPrio(), new UndefinedPrio()) > 0);
        assertTrue(testSubject.compare(new NeutralPrio(), new UndefinedPrio()) == 0);
    }

    @Test
    void sortComparisonResultsInCorrectSortOrder() {
        HighPrio highPrio = new HighPrio();
        LowPrio lowPrio = new LowPrio();
        List<Object> result = Arrays.asList(new NeutralPrio(), new UndefinedPrio(), highPrio, lowPrio);

        for (int i = 0; i < 100; i++) {
            Collections.shuffle(result);
            result.sort(testSubject);

            assertEquals(highPrio, result.get(0));
            assertEquals(lowPrio, result.get(3));
        }
    }

    @Priority(Priority.LAST)
    private static class LowPrio {
    }

    @Priority(Priority.FIRST)
    private static class HighPrio {
    }

    @Priority(Priority.NEUTRAL)
    private static class NeutralPrio {
    }

    private static class UndefinedPrio {
    }
}
