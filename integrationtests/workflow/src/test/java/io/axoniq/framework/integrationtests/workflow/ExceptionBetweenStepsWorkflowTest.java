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
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
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


/**
 * Verifies that a {@link RuntimeException} thrown in user code between steps does not put the workflow into any
 * terminal state. The workflow should remain in a non-terminal state.
 *
 * @author Stefan Dragisic
 */
class ExceptionBetweenStepsWorkflowTest extends AbstractWorkflowIntegrationTestBase<BaseWorkflowContext> {

    public ExceptionBetweenStepsWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new ExceptionBetweenStepsWorkflow());
    }

    @Test
    void exceptionBetweenStepsFailsTheWorkflow() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-exc-between", "excbetween@test.com", "vip"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStepNames().contains("stepA"));

        // An unhandled exception in the body is a defect, so the workflow ends FAILED.
        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.FAILED);
        testDriver.testingState().hasSteps("stepA");
        testDriver.testingState().noStep("stepB");
    }

    public static class ExceptionBetweenStepsWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(ExceptionBetweenStepsWorkflow.class);

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.exceptionbetween",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(BaseWorkflowContext ctx) {
            logger.info("ExceptionBetweenSteps workflow started for {}", ctx.workflowPayload());

            ctx.awaitExecute("stepA", Map.of(), (c, p) -> Map.of("result", "done"));

            logger.info("Throwing exception between steps");
            riskyComputation();

            ctx.awaitExecute("stepB", Map.of(), (c, p) -> Map.of("result", "done"));
        }

        private void riskyComputation() {
            throw new RuntimeException("Unexpected error between steps");
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
