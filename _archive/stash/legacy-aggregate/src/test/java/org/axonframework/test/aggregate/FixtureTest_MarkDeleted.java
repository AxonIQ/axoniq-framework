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

import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.test.AxonAssertionError;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Jan-Hendrik Kuperus
 */
class FixtureTest_MarkDeleted {

    private FixtureConfiguration<AnnotatedAggregate> fixture;

    @BeforeEach
    void setUp() {
        fixture = new AggregateTestFixture<>(AnnotatedAggregate.class);
    }

    @AfterEach
    void tearDown() {
        if (CurrentUnitOfWork.isStarted()) {
            fail("A unit of work is still running");
        }
    }

    @Test
    @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert") // Test succeeds when no Error is thrown
    @Disabled("TODO #3195 - Migration Module")
    void createAggregateYieldsLiveAggregate() {
        fixture.registerInjectableResource(new HardToCreateResource());
        fixture.givenNoPriorActivity()
               .when(new CreateAggregateCommand("id"))
               .expectEvents(new MyEvent("id", 0))
               .expectNotMarkedDeleted();
    }

    @Test
    void createAggregateYieldsLiveAggregateInverted() {
        fixture.registerInjectableResource(new HardToCreateResource());

        assertThrows(AxonAssertionError.class, () ->
                fixture.givenNoPriorActivity()
                        .when(new CreateAggregateCommand("id"))
                        .expectEvents(new MyEvent("id", 0))
                        .expectMarkedDeleted());
    }

    @Test
    @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert") // Test succeeds when no Error is thrown
    @Disabled("TODO #3195 - Migration Module")
    void deletedAggregateYieldsAggregateMarkedDeleted() {
        fixture.given(new MyEvent("id", 0))
               .when(new DeleteCommand("id", false))
               .expectEvents(new MyAggregateDeletedEvent(false))
               .expectMarkedDeleted();
    }

    @Test
    void deletedAggregateYieldsAggregateMarkedDeletedInverted() {
        assertThrows(AxonAssertionError.class, () ->
                fixture.given(new MyEvent("id", 0))
                        .when(new DeleteCommand("id", false))
                        .expectEvents(new MyAggregateDeletedEvent(false))
                        .expectNotMarkedDeleted());

    }

}
