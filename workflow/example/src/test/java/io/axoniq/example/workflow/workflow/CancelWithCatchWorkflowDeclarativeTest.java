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
package io.axoniq.example.workflow.declarative;

import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.execution.EventConditions;
import io.axoniq.workflow.runtime.engine.execution.WorkflowInstance;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import io.axoniq.workflow.runtime.test.configuration.PrettyPrintingRecordingEventStore;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.engine.impl.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that no further steps can be executed after a workflow has been cancelled,
 * even if the user code catches the exception.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class CancelWithCatchWorkflowDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public CancelWithCatchWorkflowDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
        var workflow = new CancelWithCatchWorkflow();
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("CancelWithCatch workflow in Java")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.cancelcatch").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "cancelcatch-" + id))
                );
    }

    @Test
    void noFurtherStepsAfterCancel() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-002", "cancelcatch@test.com", "active"))
        ));

        delayedPublisher.start();

        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowInstances()).isNotEmpty();
        });

        workflowEngine.runWorkflows();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowEngine.workflowInstances()).allMatch(h -> h.workflowStatus().isTerminal());
        });

        // Wait for async cleanup to settle
        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (WorkflowContext context : workflowEngine.workflowInstances().stream()
                                                     .map(WorkflowInstance::workflowContext).toList()) {
            assertThat(context.workflowStatus()).isEqualTo(WorkflowStatus.CANCELLED);
            assertThat(context.workflowStepNames()).contains("stepA");
            assertThat(context.workflowStepNames()).doesNotContain("stepAfterCancel");
        }

        // Verify no events were published after the workflow terminal event
        var events = PrettyPrintingRecordingEventStore.lastInstance().getPublishedEvents().stream()
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
}
