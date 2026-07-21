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
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * F-26 regression: an {@code execute} action returning {@code null} must drive the step (and workflow) to a terminal
 * state rather than wedging on a null-result dereference in the completion handler. Before the fix the step stayed
 * {@code STARTED} forever; after the fix the null result sanitizes to an empty map and the step {@code COMPLETED}.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class NullResultCompletesTest extends AbstractWorkflowTestBase<SimpleWorkflowContext> {

    public NullResultCompletesTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> new NullResultWorkflow());
    }

    @Test
    void nullActionResultCompletesInsteadOfWedging() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartNullResultEvent("null-1"))
        ));
        delayedPublisher.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var history = workflowHistoryRepository.findById("null-1");
            assertThat(history).isPresent();
            var state = history.get().state();
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(state.getStep("returnsNull").status()).isEqualTo(StepStatus.COMPLETED);
        });
    }

    public static class NullResultWorkflow {

        @Workflow(
                workflowName = "NullResultWorkflow",
                workflowNamespace = "io.axoniq.dsl.nullresult",
                idProperty = "id",
                startOnEventClass = StartNullResultEvent.class
        )
        public void execute(@Nonnull SimpleWorkflowContext ctx) {
            ctx.awaitExecute("returnsNull", Map.of(), (c, p) -> null, step -> step.timeout(Duration.ofDays(1)));
        }
    }

    @Event(namespace = "io.axoniq.nullresult", name = "StartNullResult")
    public record StartNullResultEvent(String id) {

    }
}
