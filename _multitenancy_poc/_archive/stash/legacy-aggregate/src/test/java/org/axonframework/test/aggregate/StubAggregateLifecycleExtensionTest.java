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

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;
import static org.axonframework.modelling.command.AggregateLifecycle.markDeleted;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link StubAggregateLifecycleExtension}.
 *
 * @author Stefan Dragisic
 */
class StubAggregateLifecycleExtensionTest {

    @RegisterExtension
    static final StubAggregateLifecycleExtension TEST_SUBJECT = new StubAggregateLifecycleExtension();

    @Test
    void appliedEventsArePassedToActiveLifecycle() {
        apply("test");

        assertEquals(1, TEST_SUBJECT.getAppliedEvents().size());
        assertEquals("test", TEST_SUBJECT.getAppliedEventPayloads().get(0));
        assertEquals("test", TEST_SUBJECT.getAppliedEvents().get(0).payload());
    }

    @Test
    void testMarkDeletedIsRegisteredWithActiveLifecycle() {
        markDeleted();

        assertEquals(0, TEST_SUBJECT.getAppliedEvents().size());
        assertTrue(TEST_SUBJECT.isMarkedDeleted());
    }
}
