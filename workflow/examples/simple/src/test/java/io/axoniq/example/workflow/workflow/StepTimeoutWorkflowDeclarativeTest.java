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

import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Declarative reproduction of <a href="https://github.com/AxonIQ/extension-workflow/issues/122">issue #122</a>:
 * step-level timeouts are not enforced when the step action runs past the configured timeout.
 * <p>
 * Two scenarios are covered, both with a step whose action busy-waits {@value StepTimeoutWorkflow#STEP_SLEEP_MILLIS}ms
 * (ignoring interrupts) with a 200ms timeout:
 * <ol>
 *   <li>no retry policy — mirrors the reporter's setup. Expected: step TIMED_OUT at ~200ms.</li>
 *   <li>with {@code RetryPolicy.maxRetries(5)} — expected: each of the 6 attempts respects the per-attempt
 *       timeout, so total workflow duration is at least {@code 6 * 200ms = 1200ms}.</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class StepTimeoutWorkflowDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    private final StepTimeoutWorkflow workflow = new StepTimeoutWorkflow();

    public StepTimeoutWorkflowDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("Step timeout workflow")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.steptimeout").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "timeout-" + id))
                );
    }

    @Test
    void stepTimeoutWithoutRetryMustTerminateStepAtTimeout() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("user-no-retry", "t@test.com", "no-retry"))
        ));
        delayedPublisher.start();

        long workflowStart = System.currentTimeMillis();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        long workflowDuration = System.currentTimeMillis() - workflowStart;

        var state = workflowHistoryRepository.findAll().iterator().next().state();
        assertThat(state.getStep("slowStep").status())
                .as("issue #122: step must be TIMED_OUT, not COMPLETED — got %s after %dms",
                    state.getStep("slowStep").status(), workflowDuration)
                .isEqualTo(StepStatus.TIMED_OUT);

        assertThat(workflow.stepResult().timeout())
                .as("issue #122: WorkflowStepResult#timeout() must be true")
                .isTrue();

        assertThat(workflowDuration)
                .as("issue #122: workflow must terminate near the %dms step timeout, not after the full %dms sleep",
                    StepTimeoutWorkflow.STEP_TIMEOUT.toMillis(), StepTimeoutWorkflow.STEP_SLEEP_MILLIS)
                .isLessThan(StepTimeoutWorkflow.STEP_SLEEP_MILLIS);
    }

    @Test
    void stepTimeoutWithRetryMustApplyPerAttempt() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("user-retry", "t@test.com", "retry"))
        ));
        delayedPublisher.start();

        long workflowStart = System.currentTimeMillis();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        long workflowDuration = System.currentTimeMillis() - workflowStart;

        var state = workflowHistoryRepository.findAll().iterator().next().state();
        assertThat(state.getStep("slowStep").status()).isEqualTo(StepStatus.TIMED_OUT);

        assertThat(workflow.attempts())
                .as("all %d attempts (1 initial + %d retries) must have run",
                    StepTimeoutWorkflow.MAX_RETRIES + 1, StepTimeoutWorkflow.MAX_RETRIES)
                .isEqualTo(StepTimeoutWorkflow.MAX_RETRIES + 1);

        long expectedMinDuration =
                (StepTimeoutWorkflow.MAX_RETRIES + 1) * StepTimeoutWorkflow.STEP_TIMEOUT.toMillis();
        // 20% tolerance for scheduling jitter
        long tolerance = expectedMinDuration / 5;
        assertThat(workflowDuration)
                .as("issue #122: each attempt should respect the %dms timeout, so 6 attempts must take "
                            + "at least ~%dms — observed %dms suggests retries are burning their budget instantly",
                    StepTimeoutWorkflow.STEP_TIMEOUT.toMillis(), expectedMinDuration, workflowDuration)
                .isGreaterThanOrEqualTo(expectedMinDuration - tolerance);
    }
}
