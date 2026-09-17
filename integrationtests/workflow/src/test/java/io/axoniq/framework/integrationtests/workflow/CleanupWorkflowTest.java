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
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.WorkflowFailedException;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;

/**
 * Test making sure that after the execution of a workflow, its execution is not present on the workflow engine
 * anymore.
 */
class CleanupWorkflowTest extends AbstractWorkflowIntegrationTestBase<BaseWorkflowContext> {

    public CleanupWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new CleanupWorkflow());
    }

    @Test
    void workflowIsRemovedFromEngineAfterCompletion() {
        String workflowId = "cleanup-user-001";
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("user-001", "cleanup@test.com", "active"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(history -> history.workflowId().equals(workflowId));
        testDriver.noExecution();
    }

    @Test
    void workflowIsRemovedFromEngineAfterFailure() {
        String workflowId = "cleanup-user-fail-002";
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("user-fail-002", "fail@test.com", "active"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(history -> history.workflowId().equals(workflowId));
        testDriver.noExecution();
    }

    public static class CleanupWorkflow {

        @Workflow(
                workflowName = "CleanupWorkflow",
                idPropertyProvider = CleanupWorkflowIdProvider.class,
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(BaseWorkflowContext ctx) {
            if (ctx.workflowId().contains("fail")) {
                ctx.fail(new WorkflowFailedException("Forced failure"));
            }
            ctx.awaitExecute("step1", Map.of(), (context, payload) -> Map.of("result", "done"));
        }
    }

    public static class CleanupWorkflowIdProvider extends PayloadPropertyWorkflowIdProvider {

        public CleanupWorkflowIdProvider() {
            super("id", id -> "cleanup-" + id);
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
