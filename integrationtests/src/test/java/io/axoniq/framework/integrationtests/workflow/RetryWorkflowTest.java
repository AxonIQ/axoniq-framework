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

import io.axoniq.workflow.configuration.WorkflowConfigurer;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.workflow.dsl.base.BaseWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.api.execution.context.retry.BackoffStrategy;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryContext;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractWorkflowTestBase;
import io.axoniq.workflow.runtime.test.utils.SleepUtils;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * Integration test exercising all retry scenarios: normal execution, retry-then-succeed, retry handler, cancel during
 * retry, timeout during retry, and retry exhaustion.
 *
 * @author Stefan Dragisic
 */
class RetryWorkflowTest extends AbstractWorkflowTestBase<BaseWorkflowContext> {

    private RetryWorkflow workflow;

    public RetryWorkflowTest() {
        super(BaseWorkflowContext.class, c -> new BaseWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowConfigurer> configure() {
        workflow = new RetryWorkflow();
        return super.configure();
    }

    @Override
    protected Function<DetectionPhase<BaseWorkflowContext>, FinalizedPhase<BaseWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> workflow);
    }

    @Test
    void allRetryScenarios() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("user-retry-1", "retry@test.com", "vip"))
        ));

        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);
        testDriver.testingState().hasStepsInAnyOrder(
                "normalStep",
                "retryThenSucceed",
                "retryWithHandler",
                "cancelDuringRetry",
                "timeoutDuringRetry",
                "retryExhaustion",
                "retryWithBackoff",
                "retryWhileStop"
        );

        var state = testDriver.testingState().state();
        assertThat(state.getStep("normalStep").status()).isEqualTo(StepStatus.COMPLETED);
        assertThat(state.getStep("retryThenSucceed").status()).isEqualTo(StepStatus.COMPLETED);
        assertThat(state.getStep("retryWithHandler").status()).isEqualTo(StepStatus.COMPLETED);
        assertThat(state.getStep("cancelDuringRetry").status()).isEqualTo(StepStatus.CANCELLED);
        assertThat(state.getStep("timeoutDuringRetry").status()).isEqualTo(StepStatus.TIMED_OUT);
        assertThat(state.getStep("retryExhaustion").status()).isEqualTo(StepStatus.FAILED);
        assertThat(state.getStep("retryWithBackoff").status()).isEqualTo(StepStatus.COMPLETED);
        assertThat(state.getStep("retryWhileStop").status()).isEqualTo(StepStatus.FAILED);

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

    public static class RetryWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(RetryWorkflow.class);

        private final AtomicInteger retryThenSucceedAttempts = new AtomicInteger(0);
        private final AtomicInteger retryWithHandlerAttempts = new AtomicInteger(0);
        private final AtomicInteger cancelDuringRetryAttempts = new AtomicInteger(0);
        private final AtomicInteger timeoutDuringRetryAttempts = new AtomicInteger(0);
        private final AtomicInteger retryExhaustionAttempts = new AtomicInteger(0);
        private final AtomicInteger retryWithBackoffAttempts = new AtomicInteger(0);
        private final AtomicInteger retryWhileStopAttempts = new AtomicInteger(0);
        private final CopyOnWriteArrayList<RetryContext> handlerCalls = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<Instant> backoffTimestamps = new CopyOnWriteArrayList<>();

        List<RetryContext> getHandlerCalls() {
            return handlerCalls;
        }

        List<Instant> getBackoffTimestamps() {
            return backoffTimestamps;
        }

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.retry",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(BaseWorkflowContext ctx) {
            logger.info("Retry workflow started for {}", ctx.workflowPayload());

            ctx.awaitExecute(
                    "normalStep",
                    Map.of(),
                    (c, p) -> {
                        logger.info("normalStep: executing");
                        return Map.of("normalStep", "done");
                    }
            );

            ctx.awaitExecute(
                    "retryThenSucceed",
                    Map.of(),
                    (c, p) -> {
                        int attempt = retryThenSucceedAttempts.incrementAndGet();
                        logger.info("retryThenSucceed: attempt {}", attempt);
                        if (attempt <= 2) {
                            throw new RuntimeException("retryThenSucceed failure on attempt " + attempt);
                        }
                        return Map.of("retryThenSucceed", "done");
                    },
                    step -> step.retryPolicy(RetryPolicy.maxRetries(3))
            );

            ctx.awaitExecute(
                    "retryWithHandler",
                    Map.of(),
                    (c, p) -> {
                        int attempt = retryWithHandlerAttempts.incrementAndGet();
                        logger.info("retryWithHandler: attempt {}", attempt);
                        if (attempt <= 1) {
                            throw new RuntimeException("retryWithHandler failure on attempt " + attempt);
                        }
                        return Map.of("retryWithHandler", "done");
                    },
                    step -> step.retryPolicy(RetryPolicy.maxRetries(2).onRetry(handlerCalls::add))
            );

            WorkflowStepResult cancelDuringRetry = ctx.execute(
                    "cancelDuringRetry",
                    Map.of(),
                    (c, p) -> {
                        int attempt = cancelDuringRetryAttempts.incrementAndGet();
                        logger.info("cancelDuringRetry: attempt {}", attempt);
                        if (attempt == 1) {
                            throw new RuntimeException("cancelDuringRetry failure on attempt 1");
                        }
                        throw new StepCancellationException("cancelDuringRetry cancelled on attempt " + attempt);
                    },
                    step -> step.retryPolicy(RetryPolicy.maxRetries(3))
            );
            cancelDuringRetry.await();

            WorkflowStepResult timeoutDuringRetry = ctx.execute(
                    "timeoutDuringRetry",
                    Map.of(),
                    (c, p) -> {
                        int attempt = timeoutDuringRetryAttempts.incrementAndGet();
                        logger.info("timeoutDuringRetry: attempt {}", attempt);
                        SleepUtils.sleepQuietly(500);
                        throw new RuntimeException("timeoutDuringRetry failure on attempt " + attempt);
                    },
                    step -> step.retryPolicy(RetryPolicy.maxRetries(5)).timeout(Duration.ofMillis(300))
            );
            timeoutDuringRetry.await();

            WorkflowStepResult retryExhaustion = ctx.execute(
                    "retryExhaustion",
                    Map.of(),
                    (c, p) -> {
                        int attempt = retryExhaustionAttempts.incrementAndGet();
                        logger.info("retryExhaustion: attempt {}", attempt);
                        throw new RuntimeException("retryExhaustion failure on attempt " + attempt);
                    },
                    step -> step.retryPolicy(RetryPolicy.maxRetries(3))
            );
            retryExhaustion.await();

            ctx.awaitExecute(
                    "retryWithBackoff",
                    Map.of(),
                    (c, p) -> {
                        backoffTimestamps.add(Instant.now());
                        int attempt = retryWithBackoffAttempts.incrementAndGet();
                        logger.info("retryWithBackoff: attempt {}", attempt);
                        if (attempt <= 2) {
                            throw new RuntimeException("retryWithBackoff failure on attempt " + attempt);
                        }
                        return Map.of("retryWithBackoff", "done");
                    },
                    step -> step.retryPolicy(
                            RetryPolicy.maxRetries(3)
                                       .withBackoff(BackoffStrategy.fixed(Duration.ofMillis(200)))
                    )
            );

            WorkflowStepResult retryWhileStop = ctx.execute(
                    "retryWhileStop",
                    Map.of(),
                    (c, p) -> {
                        int attempt = retryWhileStopAttempts.incrementAndGet();
                        throw new RuntimeException("retryWhileStop failure on attempt " + attempt);
                    },
                    step -> step.retryPolicy(RetryPolicy
                                                     .maxRetries(5)
                                                     .retryWhile(rc -> rc.attempt() < 2))
            );
            retryWhileStop.await();

            logger.info("Retry workflow completed");
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
