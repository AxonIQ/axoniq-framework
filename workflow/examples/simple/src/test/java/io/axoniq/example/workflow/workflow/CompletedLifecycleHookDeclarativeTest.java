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
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.OrderPlacedEvent;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression test for issue #218: on the happy completion path the COMPLETED lifecycle hook must fire.
 * <p>
 * Before the fix, {@code SimpleWorkflowExecution.executeWorkflow(...)} awaited only the COMPLETED event
 * commit and then returned, letting {@code finishWorkflow} run {@code taskQueue.clear()} — which could
 * discard the still-queued live evolution that flips the state to {@link WorkflowStatus#COMPLETED} and, with
 * it, the {@link WorkflowStatusChangeListener} notification. As a result the COMPLETED lifecycle hook was
 * silently dropped (a non-deterministic race). The fix awaits the terminal state change before returning,
 * symmetric with the fail/cancel/timeout paths, so the COMPLETED hook fires reliably exactly once.
 *
 * @author Stefan Dragisic
 */
class CompletedLifecycleHookDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    /**
     * Records every COMPLETED notification observed for the running workflow. The drift this test guards
     * against is the hook being dropped entirely, so the assertion below requires it to have fired exactly
     * once by the time the workflow reaches its terminal state.
     */
    private final AtomicInteger completedHookInvocations = new AtomicInteger(0);
    private final AtomicReference<WorkflowStatus> observedStatus = new AtomicReference<>();

    public CompletedLifecycleHookDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        // A minimal workflow that completes on the happy path (two synchronous steps, no event waits).
        var workflow = new VersionedWorkflow();
        WorkflowStatusChangeListener completedListener = new WorkflowStatusChangeListener() {
            @Override
            public <C extends WorkflowContext> void onWorkflowStatus(@Nonnull WorkflowStatus state,
                                                                     @Nonnull C context) {
                observedStatus.set(state);
                completedHookInvocations.incrementAndGet();
            }
        };
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("Completed lifecycle hook workflow")
                .on(EventConditions.fromType(OrderPlacedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.completedhook").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "orderId", id -> "order-" + id))
                        .registerWorkflowStatusChangeListener(WorkflowStatus.COMPLETED, completedListener)
                );
    }

    @Test
    void completedLifecycleHookFiresOnHappyCompletionPath() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new OrderPlacedEvent("hook-1", "customer-1"))
        ));
        delayedPublisher.start();

        // Drive the workflow to its terminal (COMPLETED) state.
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(e -> e.state().workflowStatus().isTerminal());
        });

        assertThat(workflowHistoryRepository.findAll()).hasSize(1);
        assertThat(workflowHistoryRepository.findAll())
                .allMatch(e -> e.state().workflowStatus() == WorkflowStatus.COMPLETED);

        // Regression assertion: the COMPLETED lifecycle hook must have fired exactly once by the time the
        // workflow reached its terminal state. Without the await on the happy completion path the queued
        // evolution that flips the status to COMPLETED (and thus the listener notification) is cleared by
        // finishWorkflow before it runs, dropping this hook.
        assertThat(completedHookInvocations.get())
                .as("COMPLETED lifecycle hook should fire exactly once on the happy completion path")
                .isEqualTo(1);
        assertThat(observedStatus.get()).isEqualTo(WorkflowStatus.COMPLETED);
    }
}
