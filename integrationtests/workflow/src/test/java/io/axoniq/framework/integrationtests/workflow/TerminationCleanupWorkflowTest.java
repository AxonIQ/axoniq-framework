/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.dsl.api.Payload;
import io.axoniq.framework.workflow.dsl.api.WorkflowFailedException;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.annotation.Workflow;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;

/**
 * Verifies that the termination callback of the process instance (removing it from executionRepository) is executed in
 * all possible terminal states of the workflow (complete, fail, timeout, cancel).
 */
class TerminationCleanupWorkflowTest extends AbstractWorkflowIntegrationTestBase<SimpleWorkflowContext> {

    public TerminationCleanupWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new CleanupTestWorkflow());
    }

    @Test
    void shouldCleanupOnComplete() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartEvent("1", "complete"))
        ));
        delayedPublisher.start();

        assertCleanedUpWith(WorkflowStatus.COMPLETED);
    }

    @Test
    void shouldCleanupOnFail() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartEvent("2", "fail"))
        ));
        delayedPublisher.start();

        assertCleanedUpWith(WorkflowStatus.FAILED);
    }

    @Disabled("Not supported yet")
    @Test
    void shouldCleanupOnTimeout() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartEvent("3", "timeout"))
        ));
        delayedPublisher.start();

        assertCleanedUpWith(WorkflowStatus.TIMED_OUT);
    }

    @Test
    void shouldCleanupOnCancel() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartEvent("4", "cancel"))
        ));
        delayedPublisher.start();

        assertCleanedUpWith(WorkflowStatus.CANCELLED);
    }

    private void assertCleanedUpWith(WorkflowStatus workflowStatus) {
        testDriver.historyMatches(history -> history.state().workflowStatus() == workflowStatus);
        testDriver.noExecution();
    }

    public static class CleanupTestWorkflow {

        @Workflow(
                workflowName = "CleanupTestWorkflow",
                idProperty = "id",
                startOnEventClass = StartEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            String action = (String) ctx.workflowPayload().get("action");
            if ("complete".equals(action)) {
                ctx.awaitModifyPayload(
                        "step1",
                        workflowPayload -> Payload.payload(workflowPayload)
                                                  .with(Payload.payload(Map.of("status", "done")))
                                                  .getValues()
                );
            } else if ("fail".equals(action)) {
                ctx.fail(new WorkflowFailedException("Intentional workflow failure"));
            } else if ("timeout".equals(action)) {
                // FIXME -> Support workflow timeout, currently not in MVP.
            } else if ("cancel".equals(action)) {
                ctx.cancel("Intentional cancel");
            }
        }
    }

    public record StartEvent(String id, String action) {

    }
}
