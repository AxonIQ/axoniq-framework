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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for {@link AllMatchGuardWorkflow} — verifies that
 * {@link WorkflowContext#allMatch} semantics short-circuit on the first non-match.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class AllMatchGuardWorkflowDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public AllMatchGuardWorkflowDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
        var workflow = new AllMatchGuardWorkflow();
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("AllMatch guard workflow")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.allmatch").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "guard-all-" + id))
                );
    }

    @Test
    void failingStepViolatesGuard() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-guard-all-1", "guard@test.com", "vip"))
        ));

        delayedPublisher.start();

        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).isNotEmpty();
        });

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll()).allMatch(e -> e.state().workflowStatus()
                                                                           .isTerminal());
        });

        // Wait for async cleanup to settle
        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(workflowHistoryRepository.findAll()).hasSize(1);

        for (WorkflowState state : workflowHistoryRepository.findAll().stream()
                                                              .map(WorkflowHistory::state).toList()) {
            assertThat(state.workflowStatus().isTerminal()).isTrue();
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            // Both steps should be in history
            assertThat(state.workflowStepNames()).containsExactlyInAnyOrder("successStep", "failingStep");
        }
    }
}
