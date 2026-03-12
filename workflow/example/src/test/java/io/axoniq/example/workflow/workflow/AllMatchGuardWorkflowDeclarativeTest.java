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
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.EventConditions;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
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
 * Integration test for {@link AllMatchGuardWorkflow} — verifies that
 * {@link io.axoniq.workflow.runtime.api.WorkflowContext#allMatch} semantics short-circuit on the first non-match.
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

        workflowEngine.runWorkflows(false);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).allMatch(e -> e.workflowContext().workflowStatus()
                                                                           .isTerminal());
        });

        // Wait for async cleanup to settle
        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(workflowEngine.workflowExecutions()).hasSize(1);

        for (WorkflowContext context : workflowEngine.workflowExecutions().stream()
                                                     .map(WorkflowExecution::workflowContext).toList()) {
            assertThat(context.workflowStatus().isTerminal()).isTrue();
            assertThat(context.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            // Both steps should be in history
            assertThat(context.workflowStepNames()).containsExactlyInAnyOrder("successStep", "failingStep");
        }
    }
}
