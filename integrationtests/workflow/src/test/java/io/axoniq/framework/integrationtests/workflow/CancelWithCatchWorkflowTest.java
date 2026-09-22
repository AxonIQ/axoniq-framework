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
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.test.utils.PrettyPrintingRecordingEventStore;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/**
 * Verifies that no further steps can be executed after a workflow has been cancelled, even if the user code catches the
 * exception.
 *
 * @author Stefan Dragisic
 */
class CancelWithCatchWorkflowTest extends AbstractWorkflowIntegrationTestBase<SimpleWorkflowContext> {

    public CancelWithCatchWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new CancelWithCatchWorkflow());
    }

    @Test
    void noFurtherStepsAfterCancel() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-002", "cancelcatch@test.com", "active"))
        ));

        delayedPublisher.start();

        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        testDriver.noExecution();
        if (!workflowHistoryRepository.findAll().join().isEmpty()) {
            testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.CANCELLED);
            testDriver.testingState().hasSteps("stepA");
            testDriver.testingState().noStep("stepAfterCancel");
        } else {
            fail("Expected workflow to be cancelled");
        }

        var events = PrettyPrintingRecordingEventStore
                .lastInstance()
                .recorded()
                .stream()
                .filter(e -> e.metadata().containsKey("workflowId"))
                .toList();

        // Find index of the workflow CANCELLED event
        int cancelledIndex = -1;
        for (int i = 0; i < events.size(); i++) {
            var wfStatus = MetadataUtils.getWorkflowStatus(events.get(i).metadata());
            if (wfStatus.isPresent() && wfStatus.get() == WorkflowStatus.CANCELLED) {
                cancelledIndex = i;
                break;
            }
        }
        assertThat(cancelledIndex).as("WorkflowCancelled event should exist").isGreaterThanOrEqualTo(0);

        // No workflow or step events should appear after the terminal workflow event
        List<EventMessage> eventsAfterTerminal = events.subList(cancelledIndex + 1, events.size());
        assertThat(eventsAfterTerminal)
                .as("No events should be published after WorkflowCancelled")
                .isEmpty();
    }

    public static class CancelWithCatchWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(CancelWithCatchWorkflow.class);

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.cancelcatch",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            logger.info("CancelWithCatch workflow started for {}", ctx.workflowPayload());

            ctx.awaitExecute("stepA", Map.of(), (c, p) -> Map.of("result", "done"));

            try {
                ctx.cancel("I dont want it anymore");
            } catch (WorkflowCancelledException e) {
                logger.info("Caught WorkflowCancelledException, attempting another step...");
                try {
                    ctx.awaitExecute("stepAfterCancel", Map.of(), (c, p) -> Map.of("result", "should not happen"));
                } catch (WorkflowCancelledException e2) {
                    logger.info("Guard correctly prevented step execution after cancel: {}", e2.getMessage());
                }
            }
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
