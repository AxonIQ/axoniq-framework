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
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.WorkflowFailedException;
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

/**
 * Verifies that no further steps can be executed after a workflow has been failed, even if the user code catches the
 * exception.
 *
 * @author Stefan Dragisic
 */
class FailWithCatchWorkflowTest extends AbstractWorkflowIntegrationTestBase<BaseWorkflowContext> {

    public FailWithCatchWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new FailWithCatchWorkflow());
    }

    @Test
    void noFurtherStepsAfterFail() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-001", "failcatch@test.com", "active"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.FAILED);
        testDriver.testingState().hasSteps("stepA");
        testDriver.testingState().noStep("stepAfterFail");

        // Verify no events were published after the workflow terminal event
        var events = PrettyPrintingRecordingEventStore.lastInstance().recorded().stream()
                                                      .filter(e -> e.metadata().containsKey("workflowId"))
                                                      .toList();

        // Find index of the workflow FAILED event
        int failedIndex = -1;
        for (int i = 0; i < events.size(); i++) {
            var wfStatus = MetadataUtils.getWorkflowStatus(events.get(i).metadata());
            if (wfStatus.isPresent() && wfStatus.get() == WorkflowStatus.FAILED) {
                failedIndex = i;
                break;
            }
        }
        assertThat(failedIndex).as("WorkflowFailed event should exist").isGreaterThanOrEqualTo(0);

        // No workflow or step events should appear after the terminal workflow event
        List<EventMessage> eventsAfterTerminal = events.subList(failedIndex + 1, events.size());
        assertThat(eventsAfterTerminal)
                .as("No events should be published after WorkflowFailed")
                .isEmpty();
    }

    public static class FailWithCatchWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(FailWithCatchWorkflow.class);

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.failcatch",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(BaseWorkflowContext ctx) {
            logger.info("FailWithCatch workflow started for {}", ctx.workflowPayload());

            ctx.awaitExecute("stepA", Map.of(), (c, p) -> Map.of("result", "done"));

            try {
                ctx.fail(new RuntimeException("Simulated failure"));
            } catch (WorkflowFailedException e) {
                logger.info("Caught WorkflowFailedException, attempting another step...");
                try {
                    ctx.awaitExecute("stepAfterFail", Map.of(), (c, p) -> Map.of("result", "should not happen"));
                } catch (WorkflowFailedException e2) {
                    logger.info("Guard correctly prevented step execution after fail: {}", e2.getMessage());
                }
            }
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
