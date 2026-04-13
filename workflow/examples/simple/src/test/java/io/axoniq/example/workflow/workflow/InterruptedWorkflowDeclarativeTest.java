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
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
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
 * Verifies that an {@link InterruptedException} occurring during workflow execution does not put the workflow into any
 * terminal state. The workflow should remain in a non-terminal state.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class InterruptedWorkflowDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public InterruptedWorkflowDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        var workflow = new InterruptedWorkflow();
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("Interrupted workflow in Java")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.interrupted").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "interrupted-" + id))
                );
    }

    @Test
    void interruptDoesNotTerminateWorkflow() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-interrupted", "interrupted@test.com", "vip"))
        ));

        delayedPublisher.start();

        // Wait for stepA to complete in history (confirms workflow ran)
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .anyMatch(h -> h.state().workflowStepNames().contains("stepA"));
        });

        // Wait for execution to be cleaned up
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                                                                   assertThat(workflowEngine.workflowExecutions()).isEmpty()
        );

        assertThat(workflowHistoryRepository.findAll()).hasSize(1);

        for (WorkflowHistory history : workflowHistoryRepository.findAll()) {
            var state = history.state();
            // InterruptedException should NOT put workflow into any terminal state
            assertThat(state.workflowStatus().isTerminal())
                    .as("InterruptedException should not put workflow into any terminal state, but was: %s",
                        state.workflowStatus())
                    .isFalse();
            // stepA completed before the interrupt
            assertThat(state.workflowStepNames()).contains("stepA");
            // No steps should be started after the interrupt — interruption must be immediate
            assertThat(state.workflowStepNames())
                    .as("No steps should be started after interruption, but found: %s", state.workflowStepNames())
                    .doesNotContain("stepB");
        }
    }
}
