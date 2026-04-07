/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test exercising all retry scenarios: normal execution, retry-then-succeed,
 * retry handler, cancel during retry, timeout during retry, and retry exhaustion.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class RetryWorkflowDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    private final RetryWorkflow workflow = new RetryWorkflow();

    public RetryWorkflowDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("Retry workflow in Java")
                .on(EventConditions.fromType(RegistrationReceivedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.retry").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "retry-" + id))
                );
    }

    @Test
    void allRetryScenarios() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-retry-1", "retry@test.com", "vip"))
        ));

        delayedPublisher.start();

        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).isNotEmpty();
        });

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        // Wait 2 seconds before asserting to let async cleanup settle
        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(workflowHistoryRepository.findAll()).hasSize(1);

        for (var history : workflowHistoryRepository.findAll()) {
            var state = history.state();

            // Workflow completes normally (steps 4-6 use execute() which doesn't propagate step failures)
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);

            // All 8 steps present
            assertThat(state.workflowStepNames()).containsExactlyInAnyOrder(
                    "normalStep",
                    "retryThenSucceed",
                    "retryWithHandler",
                    "cancelDuringRetry",
                    "timeoutDuringRetry",
                    "retryExhaustion",
                    "retryWithBackoff",
                    "retryWhileStop"
            );

            // Step 1: Normal step — COMPLETED
            assertThat(state.getStep("normalStep").status()).isEqualTo(StepStatus.COMPLETED);

            // Step 2: Retry then succeed — COMPLETED (failed 2x, succeeded on attempt 3)
            assertThat(state.getStep("retryThenSucceed").status()).isEqualTo(StepStatus.COMPLETED);

            // Step 3: Retry with handler — COMPLETED (failed 1x, handler called, succeeded on attempt 2)
            assertThat(state.getStep("retryWithHandler").status()).isEqualTo(StepStatus.COMPLETED);

            // Step 4: Cancel during retry — CANCELLED (StepCancellationException on attempt 2)
            assertThat(state.getStep("cancelDuringRetry").status()).isEqualTo(StepStatus.CANCELLED);

            // Step 5: Timeout during retry — TIMED_OUT (per-attempt 300ms timeout, attempt sleeps 500ms)
            assertThat(state.getStep("timeoutDuringRetry").status()).isEqualTo(StepStatus.TIMED_OUT);

            // Step 6: Retry exhaustion — FAILED (all retries exhausted)
            assertThat(state.getStep("retryExhaustion").status()).isEqualTo(StepStatus.FAILED);

            // Step 7: Retry with backoff — COMPLETED (failed 2x with 200ms backoff, succeeded on attempt 3)
            assertThat(state.getStep("retryWithBackoff").status()).isEqualTo(StepStatus.COMPLETED);

            // Step 8: retryWhile — FAILED (predicate stopped retrying after attempt 2)
            assertThat(state.getStep("retryWhileStop").status()).isEqualTo(StepStatus.FAILED);

        }

        // Retry handler was invoked exactly once, for the retryWithHandler step
        assertThat(workflow.getHandlerCalls()).hasSize(1);
        assertThat(workflow.getHandlerCalls().get(0).stepName()).isEqualTo("retryWithHandler");
        assertThat(workflow.getHandlerCalls().get(0).attempt()).isEqualTo(1);
        assertThat(workflow.getHandlerCalls().get(0).maxRetries()).isEqualTo(2);

        // Verify backoff timing: 3 attempts, gaps between consecutive attempts >= 200ms (with tolerance)
        var timestamps = workflow.getBackoffTimestamps();
        assertThat(timestamps).hasSize(3);
        for (int i = 1; i < timestamps.size(); i++) {
            long gapMs = java.time.Duration.between(timestamps.get(i - 1), timestamps.get(i)).toMillis();
            assertThat(gapMs).as("Gap between attempt %d and %d should be >= 150ms (200ms with tolerance)", i, i + 1)
                             .isGreaterThanOrEqualTo(150);
        }
    }
}
