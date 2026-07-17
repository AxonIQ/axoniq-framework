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
import io.axoniq.workflow.runtime.api.execution.context.retry.BackoffStrategy;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.test.AbstractWorkflowTestBase;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that cancelling a retrying step while it is parked in its backoff window is honored: the step must reach
 * CANCELLED, the doomed retry attempt must never run its action again, and a workflow that catches the cancellation
 * must be able to compensate and complete.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class BackoffWindowCancellationWorkflowTest extends AbstractWorkflowTestBase<SimpleWorkflowContext> {

    private static final String WORKFLOW_ID = "backoff-cancel-1";
    private static final Duration BACKOFF = Duration.ofSeconds(30);

    private BackoffCancelWorkflow workflow;

    public BackoffWindowCancellationWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<io.axoniq.workflow.configuration.WorkflowConfigurer> configure() {
        workflow = new BackoffCancelWorkflow();
        return super.configure();
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> workflow);
    }

    @Test
    void cancellingStepInBackoffWindowCancelsItAndSkipsTheRetryAttempt() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new RegistrationReceivedEvent(WORKFLOW_ID, "backoff@test.com", "active"))
        ));
        delayedPublisher.start();

        var executionRepository = configuration.getComponent(WorkflowExecutionRepository.class);

        // Wait until attempt 1 has failed and the step is parked RETRYING in its 30s backoff window.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var execution = executionRepository.findById(WORKFLOW_ID);
            assertThat(execution).isPresent();
            assertThat(execution.get().state().containsStep("flaky")).isTrue();
            assertThat(execution.get().state().getStep("flaky").status()).isEqualTo(StepStatus.RETRYING);
        });
        assertThat(workflow.attempts()).isEqualTo(1);

        // Cancel the step while it is parked in the backoff window.
        executionRepository.findById(WORKFLOW_ID).orElseThrow()
                           .cancelRunningStep("flaky", new StepCancellationException("cancelled during backoff"));

        // The cancellation must be honored well within the 30s backoff window: step CANCELLED, body catches and
        // compensates, workflow completes. No second attempt may ever run.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var history = workflowHistoryRepository.findAll().stream().findFirst();
            assertThat(history).isPresent();
            var state = history.get().state();
            assertThat(state.getStep("flaky").status()).isEqualTo(StepStatus.CANCELLED);
            assertThat(state.getStep("compensate").status()).isEqualTo(StepStatus.COMPLETED);
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        });
        assertThat(workflow.attempts())
                .as("the retry attempt of a cancelled step must never run")
                .isEqualTo(1);
    }

    public static class BackoffCancelWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(BackoffCancelWorkflow.class);

        private final AtomicInteger attempts = new AtomicInteger(0);

        int attempts() {
            return attempts.get();
        }

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.backoffcancel",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(@Nonnull SimpleWorkflowContext ctx) {
            logger.info("BackoffCancelWorkflow started for {}", ctx.workflowPayload());
            try {
                ctx.awaitExecute(
                        "flaky",
                        Map.of(),
                        (c, p) -> {
                            int attempt = attempts.incrementAndGet();
                            logger.info("flaky: attempt {}", attempt);
                            if (attempt == 1) {
                                throw new RuntimeException("flaky failure on attempt 1");
                            }
                            return Map.of("flaky", "done");
                        },
                        step -> step.retryPolicy(
                                RetryPolicy.maxRetries(3).withBackoff(BackoffStrategy.fixed(BACKOFF)))
                );
            } catch (StepCancellationException cancelled) {
                logger.info("flaky cancelled, compensating: {}", cancelled.getMessage());
                ctx.awaitExecute("compensate", Map.of(), (c, p) -> Map.of("compensated", true));
            }
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
