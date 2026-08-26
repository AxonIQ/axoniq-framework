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

import io.axoniq.workflow.configuration.WorkflowConfigurer;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.workflow.dsl.base.BaseWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryContext;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.StepTimedOutException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.test.AbstractWorkflowTestBase;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

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
class StepTimeoutWorkflowTest extends AbstractWorkflowTestBase<BaseWorkflowContext> {

    private StepTimeoutWorkflow workflow;

    public StepTimeoutWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowConfigurer> configure() {
        this.workflow = new StepTimeoutWorkflow();
        return super.configure();
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> workflow);
    }

    @Test
    void stepTimeoutWithoutRetryMustTerminateStepAtTimeout() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent("user-no-retry", "t@test.com", "no-retry"))
        ));
        delayedPublisher.start();

        long workflowStart = System.currentTimeMillis();

        testDriver.historyMatches(h -> h.state().workflowStatus().isTerminal());

        long workflowDuration = System.currentTimeMillis() - workflowStart;

        var state = testDriver.testingState().state();
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

        testDriver.historyMatches(h -> h.state().workflowStatus().isTerminal());

        long workflowDuration = System.currentTimeMillis() - workflowStart;

        var state = testDriver.testingState().state();
        assertThat(state.getStep("slowStep").status()).isEqualTo(StepStatus.TIMED_OUT);

        assertThat(workflow.attempts())
                .as("all %d attempts (1 initial + %d retries) must have run",
                    StepTimeoutWorkflow.MAX_RETRIES + 1, StepTimeoutWorkflow.MAX_RETRIES)
                .isEqualTo(StepTimeoutWorkflow.MAX_RETRIES + 1);

        assertThat(workflow.retryContexts())
                .as("each timed-out attempt must supply its timeout error to the retry policy")
                .hasSize(StepTimeoutWorkflow.MAX_RETRIES)
                .allSatisfy(context -> assertThat(context.error()).isInstanceOf(StepTimedOutException.class));

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

    public static class StepTimeoutWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(StepTimeoutWorkflow.class);

        static final Duration STEP_TIMEOUT = Duration.ofMillis(200);
        static final long STEP_SLEEP_MILLIS = 1500L;
        static final int MAX_RETRIES = 5;

        private final AtomicInteger attempts = new AtomicInteger(0);
        private final List<RetryContext> retryContexts = new CopyOnWriteArrayList<>();
        private volatile WorkflowStepResult stepResult;

        int attempts() {
            return attempts.get();
        }

        WorkflowStepResult stepResult() {
            return stepResult;
        }

        List<RetryContext> retryContexts() {
            return retryContexts;
        }

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.steptimeout",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(@Nonnull BaseWorkflowContext ctx) {
            logger.info("StepTimeoutWorkflow started for {}", ctx.workflowPayload());

            boolean useRetry = "retry".equals(ctx.workflowPayload().get("status"));

            if (useRetry) {
                stepResult = ctx.execute(
                        "slowStep",
                        Map.of(),
                        this::slowAction,
                        step -> step.timeout(STEP_TIMEOUT)
                                    .retryPolicy(RetryPolicy.maxRetries(MAX_RETRIES).onRetry(retryContexts::add))
                );
            } else {
                stepResult = ctx.execute(
                        "slowStep",
                        Map.of(),
                        this::slowAction,
                        step -> step.timeout(STEP_TIMEOUT)
                );
            }

            stepResult.await();

            logger.info("StepTimeoutWorkflow completed");
        }

        private Map<String, Object> slowAction(ProcessingContext context, Map<String, Object> payload) {
            int attempt = attempts.incrementAndGet();
            long started = System.currentTimeMillis();
            double random = Math.random();
            logger.info("slowStep: attempt {} - busy-waiting for {}ms (random={})",
                        attempt, STEP_SLEEP_MILLIS, random);
            long deadline = started + STEP_SLEEP_MILLIS;
            while (System.currentTimeMillis() < deadline) {
                Thread.interrupted();
            }
            long elapsed = System.currentTimeMillis() - started;
            logger.info("slowStep: attempt {} - returning after {}ms (timeout was {}ms)",
                        attempt, elapsed, STEP_TIMEOUT.toMillis());
            return Map.of("slowStep", "done", "random", random, "elapsedMs", elapsed);
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
