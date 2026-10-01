/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.workflow.runtime.test.fixture;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * A step started after {@code timePasses(..)} measures its timeout from the fixture clock, not from the system
 * clock. Before, the step's start time came from the event timestamp (system clock) while the deadline check used
 * the advanced fixture clock, so any step with a timeout shorter than the skipped time timed out at once.
 */
class WorkflowStepClockAfterTimePassesTest {

    private static final Duration WAIT = Duration.ofDays(2);
    private WorkflowTestFixture<?, ?> fixture;

    @BeforeEach
    void setUp() {
        var module = WorkflowModule.defaults("ClockAfterTimePasses", SimpleWorkflowContext.class)
                                   .definition(d -> d.autodetected(c -> new ReminderWorkflow()));
        fixture = WorkflowTestFixture.of(module, UnaryOperator.identity());
    }

    @AfterEach
    void tearDown() {
        fixture.then().stop();
    }

    @Test
    void aStepStartedAfterALongTimeJumpUsesItsOwnTimeout() {
        fixture.when().publishEvent(new ReviewRequested("r-1"));
        fixture.then().executionExists().waitingIn("waitForReview");

        fixture.when().timePasses(WAIT.plusMinutes(1));
        fixture.then().step("waitForReview", StepStatus.TIMED_OUT).waitingIn("remind");

        // The default execute timeout is 5 seconds, far shorter than the 2 days skipped above.
        fixture.when().executeReturning("remind", Map.of());
        fixture.then().step("remind", StepStatus.COMPLETED).workflowFinished(WorkflowStatus.COMPLETED);
    }

    @Event(namespace = "io.axoniq.framework.workflow.test", name = "ReviewRequested")
    record ReviewRequested(String reviewId) {
    }

    public static class ReminderWorkflow {

        @Workflow(workflowName = "Reminder", workflowNamespace = "io.axoniq.framework.workflow.test.clock",
                  idProperty = "reviewId", startOnEventClass = ReviewRequested.class)
        public void execute(SimpleWorkflowContext ctx) {
            var review = ctx.waitForEvent("waitForReview", EventConditions.never(), step -> step.timeout(WAIT));
            review.await();
            if (review.timeout()) {
                ctx.awaitExecute("remind", Map.of(), (processingContext, payload) -> Map.of("reminded", true));
            }
        }
    }
}
