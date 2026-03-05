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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.EventConditions;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import io.axoniq.workflow.runtime.test.configuration.PrettyPrintingRecordingEventStore;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.engine.impl.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that no further steps can be executed after a workflow has been failed, even if the user code catches the
 * exception.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class FailWithCatchWorkflowDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public FailWithCatchWorkflowDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
        var workflow = new io.axoniq.example.workflow.declarative.FailWithCatchWorkflow();
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("FailWithCatch workflow in Java")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.failcatch").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "failcatch-" + id))
                );
    }

    @Test
    void noFurtherStepsAfterFail() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-001", "failcatch@test.com", "active"))
        ));

        delayedPublisher.start();

        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).isNotEmpty();
        });

        workflowEngine.runWorkflows();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        // Wait for async cleanup to settle
        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (WorkflowExecution execution : workflowEngine.workflowExecutions()) {
            var context = execution.workflowContext();
            assertThat(context.workflowStatus()).isEqualTo(WorkflowStatus.FAILED);
            assertThat(context.workflowStepNames()).contains("stepA");
            assertThat(context.workflowStepNames()).doesNotContain("stepAfterFail");
        }

        // Verify no events were published after the workflow terminal event
        var events = PrettyPrintingRecordingEventStore.lastInstance().getPublishedEvents().stream()
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
}
