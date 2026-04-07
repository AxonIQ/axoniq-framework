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
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
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
 * Integration test for {@link SleepAsyncWorkflow} — verifies that
 * {@link SimpleWorkflowContext#sleepAsync} returns a composable result.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class SleepAsyncWorkflowDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public SleepAsyncWorkflowDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
        var workflow = new SleepAsyncWorkflow();
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("SleepAsync workflow")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.sleepasync").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "sleep-" + id))
                );
    }

    @Test
    void sleepAsyncCompletesWithWork() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-sleep-1", "sleep@test.com", "vip"))
        ));

        delayedPublisher.start();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll()).allMatch(h ->
                    h.state().workflowStatus().isTerminal());
        });

        assertThat(workflowHistoryRepository.findAll()).hasSize(1);

        for (var history : workflowHistoryRepository.findAll()) {
            var state = history.state();
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(state.workflowStepNames()).containsExactlyInAnyOrder("cooldown", "doWork");
        }
    }
}
