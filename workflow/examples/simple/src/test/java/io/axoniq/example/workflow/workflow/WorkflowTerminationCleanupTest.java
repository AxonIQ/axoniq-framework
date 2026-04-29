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

import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that the termination callback of the process instance (removing it from executionRepository) is executed in
 * all possible terminal states of the workflow (complete, fail, timeout, cancel).
 */
class WorkflowExecutionTerminationCleanupTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public WorkflowExecutionTerminationCleanupTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .declarative(c -> this::workflowDefinition)
                .workflowName("CleanupTestWorkflow")
                .on(EventConditions.fromType(StartEvent.class))
                .customized((c, w) -> w
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "cleanup-" + id))
                );
    }

    private void workflowDefinition(SimpleWorkflowContext ctx) {
        String action = (String) ctx.workflowPayload().get("action");
        if ("complete".equals(action)) {
            ctx.setPayload("step1", Map.of("status", "done"));
        } else if ("fail".equals(action)) {
            throw new WorkflowFailedException("Intentional workflow failure");
        } else if ("timeout".equals(action)) {
            // FIXME -> Support workflow timeout, currently not in MVP.
        } else if ("cancel".equals(action)) {
            ctx.cancel("Intentional cancel");
        }
    }

    @Test
    void shouldCleanupOnComplete() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartEvent("1", "complete"))
        ));
        delayedPublisher.start();

        await().untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);
        });

        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).isEmpty();
        });
    }

    @Test
    void shouldCleanupOnFail() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartEvent("2", "fail"))
        ));
        delayedPublisher.start();

        await().untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus() == WorkflowStatus.FAILED);
        });

        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).isEmpty();
        });
    }

    @Disabled("Not supported yet")
    @Test
    void shouldCleanupOnTimeout() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartEvent("3", "timeout"))
        ));
        delayedPublisher.start();

        await().untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus() == WorkflowStatus.TIMED_OUT);
        });

        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).isEmpty();
        });
    }

    @Test
    void shouldCleanupOnCancel() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartEvent("4", "cancel"))
        ));
        delayedPublisher.start();

        await().untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus() == WorkflowStatus.CANCELLED);
        });

        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).isEmpty();
        });
    }

    public record StartEvent(String id, String action) {

    }
}