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
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class SleepAsyncIntegrationTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public SleepAsyncIntegrationTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .declarative(c -> this::execute)
                .workflowName("SleepAsyncWorkflow")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .notCustomized();
    }

    public void execute(SimpleWorkflowContext ctx) {
        var sleep = ctx.waitForEvent("cooldown", EventConditions.never(), step -> step.timeout(Duration.ofMillis(500)));
        var approval = ctx.execute(
                "approval",
                Map.of(),
                (c, p) -> Map.of("approved", true)
        );

        // Wait for both
        ctx.allMatch(WorkflowStepResult::isCompleted, sleep, approval).await();
    }

    @Test
    void shouldCompleteWorkflowWithSleepAsync() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("wf-sleep", "test@axoniq.io", "sleep"))
        ));

        delayedPublisher.start();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        var history = workflowHistoryRepository.findAll().iterator().next();
        assertThat(history.state().workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(history.state().workflowStepNames()).contains("cooldown", "approval");
    }
}
