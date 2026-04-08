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

package org.axonframework.test.aggregate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;
import static org.axonframework.modelling.command.AggregateLifecycle.markDeleted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StubAggregateLifecycleTest {
    private StubAggregateLifecycle testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new StubAggregateLifecycle();
    }

    @AfterEach
    void tearDown() {
        testSubject.close();
    }

    @Test
    void lifecycleIsNotRegisteredAutomatically() {
        assertThrows(IllegalStateException.class, () -> apply("test"));
    }

    @Test
    void applyingEventsAfterDeactivationFails() {
        testSubject.activate();
        testSubject.close();

        assertThrows(IllegalStateException.class, () -> apply("test"));
    }

    @Test
    void appliedEventsArePassedToActiveLifecycle() {
        testSubject.activate();
        apply("test");

        assertEquals(1, testSubject.getAppliedEvents().size());
        assertEquals("test", testSubject.getAppliedEventPayloads().get(0));
        assertEquals("test", testSubject.getAppliedEvents().get(0).payload());
    }

    @Test
    void markDeletedIsRegisteredWithActiveLifecycle() {
        testSubject.activate();
        markDeleted();

        assertEquals(0, testSubject.getAppliedEvents().size());
        assertTrue(testSubject.isMarkedDeleted());
    }
}
