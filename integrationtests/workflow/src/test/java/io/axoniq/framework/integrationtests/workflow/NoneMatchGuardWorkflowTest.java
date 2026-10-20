/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
 * Integration test for {@link NoneMatchGuardWorkflow} — verifies that none-match semantics short-circuit on the first
 * failure.
 *
 * @author Stefan Dragisic
 */
class NoneMatchGuardWorkflowTest extends AbstractWorkflowIntegrationTestBase<BaseWorkflowContext> {

    public NoneMatchGuardWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new NoneMatchGuardWorkflow());
    }

    @Test
    void failingStepViolatesGuard() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-guard-1", "guard@test.com", "vip"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);
        testDriver.testingState().hasStepsInAnyOrder("failingStep", "slowStep");
    }

    public static class NoneMatchGuardWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(NoneMatchGuardWorkflow.class);

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.nonematch",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(BaseWorkflowContext ctx) {
            logger.info("noneMatch() workflow started for {}", ctx.workflowPayload());

            var failingStep = ctx.execute(
                    "failingStep",
                    Map.of(),
                    (c, p) -> {
                        SleepUtils.sleepQuietly(500);
                        throw new RuntimeException("step failed");
                    },
                    step -> step.timeout(Duration.ofSeconds(10))
            );

            var slowStep = ctx.execute(
                    "slowStep",
                    Map.of(),
                    (c, p) -> {
                        SleepUtils.sleepQuietly(Duration.ofMinutes(5));
                        return Map.of("result", "slow-done");
                    },
                    step -> step.timeout(Duration.ofMinutes(5))
            );

            var guard = ctx.noneMatch(WorkflowStepResult::failure, failingStep, slowStep);

            if (guard.failure()) {
                logger.info("Guard violated by: {}", guard.getStepName());
                logger.info("Violators (matched failure predicate): {}",
                            guard.matched().stream().map(WorkflowStepResult::getStepName).toList());
                logger.info("Clean steps (unmatched): {}",
                            guard.unmatched().stream().map(WorkflowStepResult::getStepName).toList());
            }
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
