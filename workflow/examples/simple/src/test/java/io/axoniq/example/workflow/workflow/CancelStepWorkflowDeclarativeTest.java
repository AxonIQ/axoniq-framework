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
import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
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
 * Verifies that {@code cancelStep} cancels a single running step without terminating the workflow.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class CancelStepWorkflowDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public CancelStepWorkflowDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        var workflow = new CancelStepWorkflow();
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("Cancel step workflow in Java")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.cancelstep").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "cancelstep-" + id))
                );
    }

    @Test
    void singleStepIsCancelledWhileOthersRemainRunning() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-step-cancel", "step@test.com", "vip"))
        ));

        delayedPublisher.start();

        await().untilAsserted(() ->
                                      assertThat(workflowEngine.workflowExecutions()).isNotEmpty()
        );

        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() ->
                              {
                                  assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
                                  assertThat(workflowHistoryRepository.findAll()).allMatch(
                                          h -> h.state().workflowStatus().isTerminal());
                              }
               );

        // Let async cleanup settle
        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(workflowHistoryRepository.findAll()).hasSize(1);

        for (WorkflowHistory wh : workflowHistoryRepository.findAll()) {
            var state = wh.state();
            // Workflow ended as CANCELLED (via ctx.cancel())
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);

            // All three steps should be present in history
            assertThat(state.workflowStepNames()).containsExactlyInAnyOrder("stepA", "stepB", "stepC");

            // stepB was explicitly cancelled via cancelStep before the workflow-level cancel
            var stepB = wh.state().getStep("stepB");
            assertThat(stepB.status()).isEqualTo(StepStatus.CANCELLED);
        }
    }
}
