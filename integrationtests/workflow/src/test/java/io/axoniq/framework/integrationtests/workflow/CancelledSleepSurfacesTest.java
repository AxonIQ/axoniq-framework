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
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionRepository;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * F-25 regression: a cancelled {@code awaitSleep} must surface the cancellation as a {@link StepCancellationException}
 * (symmetric with {@code awaitExecute}/{@code awaitEvent}), not be silently swallowed. Before the fix the body sailed
 * past the cancelled sleep and ran its post-sleep step; after the fix the workflow catches the cancellation and
 * compensates instead, and the post-sleep step never runs.
 *
 * @author Stefan Dragisic
 */
class CancelledSleepSurfacesTest extends AbstractWorkflowIntegrationTestBase<SimpleWorkflowContext> {

    public CancelledSleepSurfacesTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> new CancelledSleepWorkflow());
    }

    @Test
    void cancelledSleepSurfacesAndIsCompensated() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartSleepEvent("sleep-1"))
        ));
        delayedPublisher.start();

        var executionRepository = configuration.getComponent(WorkflowExecutionRepository.class);

        // Wait until the workflow is parked in the sleep step.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var execution = executionRepository.findById("sleep-1");
            assertThat(execution).isPresent();
            var state = execution.get().state();
            assertThat(state.containsStep("pacingDelay")).isTrue();
            assertThat(state.getStep("pacingDelay").status()).isEqualTo(StepStatus.STARTED);
        });

        // Cancel the sleep step from the test thread — the sleep must surface, not swallow, the cancellation.
        workflowCancellationService.requestStepCancellation(
                "sleep-1", "pacingDelay", new StepCancellationException("sleep cancelled externally")
        ).join();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var history = workflowHistoryRepository.findById("sleep-1").join();
            assertThat(history).isPresent();
            var state = history.get().state();
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(state.getStep("pacingDelay").status()).isEqualTo(StepStatus.CANCELLED);
            assertThat(state.getStep("compensate").status()).isEqualTo(StepStatus.COMPLETED);
            // The post-sleep step must NOT have run — the cancellation was surfaced, not swallowed.
            assertThat(state.containsStep("afterSleep")).isFalse();
        });
    }

    public static class CancelledSleepWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(CancelledSleepWorkflow.class);

        @Workflow(
                workflowName = "CancelledSleepWorkflow",
                workflowNamespace = "io.axoniq.dsl.cancelledsleep",
                idProperty = "id",
                startOnEventClass = StartSleepEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            try {
                ctx.awaitSleep("pacingDelay", step -> step.timeout(Duration.ofMinutes(5)));
                ctx.awaitExecute("afterSleep", Map.of(), (c, p) -> Map.of("ranAfterSleep", true));
            } catch (StepCancellationException cancelled) {
                logger.info("Sleep cancelled, compensating: {}", cancelled.getMessage());
                ctx.awaitExecute("compensate", Map.of(), (c, p) -> Map.of("compensated", true));
            }
        }
    }

    @Event(namespace = "io.axoniq.cancelledsleep", name = "StartSleep")
    public record StartSleepEvent(String id) {

    }
}
