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
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContextFactory;
import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.test.utils.SleepUtils;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;

/**
 * @author Stefan Dragisic
 */
class FailWorkflowTest extends AbstractWorkflowIntegrationTestBase<BaseWorkflowContext> {

    public FailWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new FailWorkflow());
    }

    @Test
    void workflowIsFailed() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-789", "fail@test.com", "vip"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.FAILED);
        testDriver.testingState().hasStepsInAnyOrder("stepA", "stepB", "stepC");
    }

    public static class FailWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(FailWorkflow.class);

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.fail",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(BaseWorkflowContext ctx) {
            logger.info("Fail workflow started for {}", ctx.workflowPayload());

            Duration fiveMin = Duration.ofMinutes(5);
            long fiveMinMs = fiveMin.toMillis();
            var stepA = ctx.execute(
                    "stepA",
                    Map.of(),
                    (c, p) -> {
                        SleepUtils.sleepQuietly(fiveMinMs);
                        return Map.of();
                    },
                    step -> step.timeout(fiveMin)
            );
            var stepB = ctx.execute(
                    "stepB",
                    Map.of(),
                    (c, p) -> {
                        SleepUtils.sleepQuietly(fiveMinMs);
                        return Map.of();
                    },
                    step -> step.timeout(fiveMin)
            );
            var stepC = ctx.execute(
                    "stepC",
                    Map.of(),
                    (c, p) -> {
                        SleepUtils.sleepQuietly(fiveMinMs);
                        return Map.of();
                    },
                    step -> step.timeout(fiveMin)
            );

            ctx.allMatch(WorkflowStepResult::isCompleted, stepA, stepB, stepC);

            SleepUtils.sleepQuietly(5_000);
            logger.info("Failing workflow after 5 seconds");
            ctx.fail(new RuntimeException("Simulated failure"));
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
