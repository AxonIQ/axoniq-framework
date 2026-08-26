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
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractWorkflowTestBase;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * F-25 happy-path complement to {@code CancelledSleepSurfacesTest}: a {@code sleep}/{@code awaitSleep} that simply
 * times out is the sleep's <b>normal</b> completion and must return without throwing, so the body sails past it and
 * runs the following step. Only cancellation surfaces; a timed-out sleep does not.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
class SleepTimesOutNormallyTest extends AbstractWorkflowTestBase<SimpleWorkflowContext> {

    public SleepTimesOutNormallyTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> new SleepWorkflow());
    }

    @Test
    void sleepThatTimesOutReturnsNormallyAndBodyContinues() {
        delayedPublisher.addSchedules(List.of(ofMillis(100, new StartSleepEvent("sleep-ok"))));
        delayedPublisher.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var history = workflowHistoryRepository.findById("sleep-ok");
            assertThat(history).isPresent();
            var state = history.get().state();
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            // The sleep elapsed normally (TIMED_OUT) and did NOT throw, so the following step ran.
            assertThat(state.getStep("pacingDelay").status()).isEqualTo(StepStatus.TIMED_OUT);
            assertThat(state.getStep("afterSleep").status()).isEqualTo(StepStatus.COMPLETED);
            // No cancellation was surfaced, so no compensation ran.
            assertThat(state.containsStep("compensate")).isFalse();
        });
    }

    public static class SleepWorkflow {

        @Workflow(
                workflowName = "SleepTimesOutNormallyWorkflow",
                workflowNamespace = "io.axoniq.dsl.sleepnormal",
                idProperty = "id",
                startOnEventClass = StartSleepEvent.class
        )
        public void execute(@Nonnull SimpleWorkflowContext ctx) {
            try {
                ctx.awaitSleep("pacingDelay", step -> step.timeout(Duration.ofMillis(200)));
                ctx.awaitExecute("afterSleep", Map.of(), (c, p) -> Map.of("ranAfterSleep", true));
            } catch (RuntimeException surfaced) {
                // A normal sleep timeout must NOT reach here; a surfaced exception would run the compensation step.
                ctx.awaitExecute("compensate", Map.of(), (c, p) -> Map.of("compensated", true));
            }
        }
    }

    @Event(namespace = "io.axoniq.sleepnormal", name = "StartSleep")
    public record StartSleepEvent(String id) {

    }
}
