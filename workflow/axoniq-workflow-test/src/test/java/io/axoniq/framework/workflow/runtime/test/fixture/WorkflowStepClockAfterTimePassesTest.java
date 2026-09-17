package io.axoniq.framework.workflow.runtime.test.fixture;

import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.EventConditions;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
                                   .workflowContextFactory(c -> new SimpleWorkflowContextFactory())
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
