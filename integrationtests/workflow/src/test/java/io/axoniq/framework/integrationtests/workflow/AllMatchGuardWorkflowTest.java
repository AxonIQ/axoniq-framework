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
 * Integration test for {@link AllMatchGuardWorkflow} — verifies that all-match semantics short-circuit on the first
 * non-match.
 *
 * @author Stefan Dragisic
 */
class AllMatchGuardWorkflowTest extends AbstractWorkflowIntegrationTestBase<BaseWorkflowContext> {

    public AllMatchGuardWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new AllMatchGuardWorkflow());
    }

    @Test
    void failingStepViolatesGuard() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-guard-all-1", "guard@test.com", "vip"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);
        testDriver.testingState().hasStepsInAnyOrder("successStep", "failingStep");
    }

    public static class AllMatchGuardWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(AllMatchGuardWorkflow.class);

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.allmatch",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(BaseWorkflowContext ctx) {
            logger.info("allMatch() workflow started for {}", ctx.workflowPayload());

            var successStep = ctx.execute(
                    "successStep",
                    Map.of(),
                    (c, p) -> {
                        SleepUtils.sleepQuietly(500);
                        return Map.of("result", "ok");
                    },
                    step -> step.timeout(Duration.ofSeconds(10))
            );

            var failingStep = ctx.execute(
                    "failingStep",
                    Map.of(),
                    (c, p) -> {
                        SleepUtils.sleepQuietly(500);
                        throw new RuntimeException("step failed");
                    },
                    step -> step.timeout(Duration.ofSeconds(10))
            );

            var guard = ctx.allMatch(WorkflowStepResult::success, successStep, failingStep);

            if (guard.failure()) {
                logger.info("Guard violated - not all steps succeeded: {}", guard.getStepName());
                logger.info("Successful steps (matched): {}",
                            guard.matched().stream().map(WorkflowStepResult::getStepName).toList());
                logger.info("Violators (unmatched): {}",
                            guard.unmatched().stream().map(WorkflowStepResult::getStepName).toList());
            }
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
