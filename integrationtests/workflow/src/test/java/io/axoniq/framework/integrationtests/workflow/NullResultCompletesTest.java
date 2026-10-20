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
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.annotation.Workflow;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * F-26 regression: an {@code execute} action returning {@code null} must drive the step (and workflow) to a terminal
 * state rather than wedging on a null-result dereference in the completion handler. Before the fix the step stayed
 * {@code STARTED} forever; after the fix the null result sanitizes to an empty map and the step {@code COMPLETED}.
 *
 * @author Stefan Dragisic
 */
class NullResultCompletesTest extends AbstractWorkflowIntegrationTestBase<SimpleWorkflowContext> {

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
            var history = workflowHistoryRepository.findById("null-1").join();
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
        public void execute(SimpleWorkflowContext ctx) {
            ctx.awaitExecute("returnsNull", Map.of(), (c, p) -> null, step -> step.timeout(Duration.ofDays(1)));
        }
    }

    @Event(namespace = "io.axoniq.nullresult", name = "StartNullResult")
    public record StartNullResultEvent(String id) {

    }
}
