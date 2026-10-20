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
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContextFactory;
import io.axoniq.framework.workflow.annotation.Workflow;
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
 * Integration test for {@link SleepAsyncWorkflow} — verifies that A wait step with {@link EventConditions#never()}
 * returns a composable result.
 *
 * @author Stefan Dragisic
 */
class SleepAsyncAnyMatchWorkflowTest extends AbstractWorkflowIntegrationTestBase<BaseWorkflowContext> {

    public SleepAsyncAnyMatchWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new SleepAsyncWorkflow());
    }

    @Test
    void sleepAsyncCompletesWithWork() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-sleep-1", "sleep@test.com", "vip"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);
        testDriver.testingState().hasStepsInAnyOrder("cooldown", "doWork");
    }

    public static class SleepAsyncWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(SleepAsyncWorkflow.class);

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.sleepasync",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(BaseWorkflowContext ctx) {
            logger.info("sleepAsync workflow started for {}", ctx.workflowPayload());

            var delay = ctx.sleep("cooldown", step -> step.timeout(Duration.ofSeconds(5)));

            var work = ctx.execute(
                    "doWork",
                    Map.of(),
                    (c, p) -> Map.of("done", true),
                    step -> step.timeout(Duration.ofSeconds(10))
            );

            ctx.anyMatch(WorkflowStepResult::isCompleted, delay, work).await();

            logger.info("sleepAsync workflow completed");
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
