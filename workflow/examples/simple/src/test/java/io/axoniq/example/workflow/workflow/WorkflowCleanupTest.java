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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Test making sure that after the execution of a workflow, its execution is not present on the workflow engine
 * anymore.
 */
class WorkflowCleanupTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public WorkflowCleanupTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
        return d -> d
                .declarative(c -> (SimpleWorkflowContext ctx) -> {
                    if (ctx.workflowId().contains("fail")) {
                        throw new WorkflowFailedException("Forced failure");
                    }
                    ctx.awaitExecute("step1", Map.of(), (context, payload) -> Map.of("result", "done"));
                })
                .workflowName("CleanupWorkflow")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "cleanup-" + id))
                );
    }

    @Test
    void workflowIsRemovedFromEngineAfterCompletion() {
        String workflowId = "cleanup-user-001";
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("user-001", "cleanup@test.com", "active"))
        ));

        delayedPublisher.start();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).isEmpty();
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .anyMatch(e -> e.workflowId().equals(workflowId))
                    .isNotEmpty();
        });
    }

    @Test
    void workflowIsRemovedFromEngineAfterFailure() {
        String workflowId = "cleanup-user-fail-002";
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("user-fail-002", "fail@test.com", "active"))
        ));

        delayedPublisher.start();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).isEmpty();
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .anyMatch(e -> e.workflowId().equals(workflowId))
                    .isNotEmpty();
        });
    }
}
