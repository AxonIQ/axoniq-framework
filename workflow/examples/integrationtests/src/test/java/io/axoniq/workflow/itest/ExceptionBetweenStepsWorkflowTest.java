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
package io.axoniq.workflow.itest;

import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.workflow.dsl.base.BaseWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.test.AbstractWorkflowTestBase;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * Verifies that a {@link RuntimeException} thrown in user code between steps does not put the workflow into any
 * terminal state. The workflow should remain in a non-terminal state.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class ExceptionBetweenStepsWorkflowTest extends AbstractWorkflowTestBase<BaseWorkflowContext> {

    public ExceptionBetweenStepsWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new ExceptionBetweenStepsWorkflow());
    }

    @Test
    void exceptionBetweenStepsDoesNotTerminateWorkflow() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-exc-between", "excbetween@test.com", "vip"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStepNames().contains("stepA"));

        var state = testDriver.testingState().state();
        assertThat(state.workflowStatus().isTerminal())
                .as("Exception between steps should not put workflow into any terminal state, but was: %s",
                    state.workflowStatus())
                .isFalse();
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
        public void execute(@Nonnull BaseWorkflowContext ctx) {
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
