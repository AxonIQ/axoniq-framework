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
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContextFactory;
import io.axoniq.framework.workflow.annotation.Workflow;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that an {@link InterruptedException} occurring during workflow execution does not put the workflow into any
 * terminal state. The workflow should remain in a non-terminal state.
 *
 * @author Stefan Dragisic
 */
class InterruptedWorkflowTest extends AbstractWorkflowIntegrationTestBase<BaseWorkflowContext> {

    public InterruptedWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new InterruptedWorkflow());
    }

    @Test
    void interruptDoesNotTerminateWorkflow() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-interrupted", "interrupted@test.com", "vip"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStepNames().contains("stepA"));

        var state = testDriver.testingState().state();
        assertThat(state.workflowStatus().isTerminal())
                .as("InterruptedException should not put workflow into any terminal state, but was: %s",
                    state.workflowStatus())
                .isFalse();
        testDriver.testingState().hasSteps("stepA");
        testDriver.testingState().noStep("stepB");
    }

    public static class InterruptedWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(InterruptedWorkflow.class);

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.interrupted",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(BaseWorkflowContext ctx) {
            logger.info("Interrupted workflow started for {}", ctx.workflowPayload());

            ctx.awaitExecute("stepA", Map.of(), (c, p) -> Map.of("result", "done"));

            logger.info("Interrupting workflow thread");
            Thread.currentThread().interrupt();

            ctx.awaitExecute("stepB", Map.of(), (c, p) -> Map.of("result", "done"));
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
