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
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #127: when a workflow reaches any terminal state while an async step is still
 * running, the engine must cancel that step so its terminal event is written before
 * the workflow itself transitions. Covers all four paths in
 * {@code SimpleWorkflowExecution}: normal completion, {@code ctx.fail(...)}, and
 * {@code ctx.cancel(...)}.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class CompleteWithRunningStepDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public CompleteWithRunningStepDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        var workflow = new CompleteWithRunningStepWorkflow();
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("CompleteWithRunningStep workflow")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.completerunning").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "complete-running-" + id))
                );
    }

    @Test
    void runningStepIsCancelledOnCompletion() {
        runAndAssertBackgroundCancelled("user-cr-1", "vip", WorkflowStatus.COMPLETED);
    }

    @Test
    void runningStepIsCancelledOnFail() {
        runAndAssertBackgroundCancelled("user-cr-2", "FAIL", WorkflowStatus.FAILED);
    }

    @Test
    void runningStepIsCancelledOnExplicitCancel() {
        runAndAssertBackgroundCancelled("user-cr-3", "CANCEL", WorkflowStatus.CANCELLED);
    }

    private void runAndAssertBackgroundCancelled(String userId, String status,
                                                 WorkflowStatus expectedWorkflowStatus) {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent(userId, userId + "@test.com", status))
        ));
        delayedPublisher.start();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        assertThat(workflowHistoryRepository.findAll()).hasSize(1);

        for (var history : workflowHistoryRepository.findAll()) {
            var state = history.state();
            assertThat(state.workflowStatus()).isEqualTo(expectedWorkflowStatus);
            assertThat(state.workflowStepNames()).contains("background");
            assertThat(state.getStep("background").status()).isEqualTo(StepStatus.CANCELLED);
        }
    }
}
