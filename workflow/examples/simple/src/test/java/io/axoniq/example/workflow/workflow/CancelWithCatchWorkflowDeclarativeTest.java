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
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import io.axoniq.workflow.runtime.test.configuration.PrettyPrintingRecordingEventStore;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that no further steps can be executed after a workflow has been cancelled, even if the user code catches the
 * exception.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class CancelWithCatchWorkflowDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public CancelWithCatchWorkflowDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
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

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        // Wait for async cleanup to settle
        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).isEmpty();
        });

        for (WorkflowHistory history : workflowHistoryRepository.findAll()) {
            var state = history.state();
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.CANCELLED);
            assertThat(state.workflowStepNames()).contains("stepA");
            assertThat(state.workflowStepNames()).doesNotContain("stepAfterCancel");
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
}
