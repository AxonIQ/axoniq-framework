/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. You may not use this file except in compliance
 * with the License.
 *
 * You may obtain a copy of the License at:
 * https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 * https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.BackoffStrategy;
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepInterruptedException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests retry-attempt scheduling independently from workflow integration tests.
 *
 * @author Simon Zambrovski
 */
class RetryableExecuteDelegateTest {

    private static final Instant NOW = Instant.parse("2026-08-18T10:00:00Z");
    private static final String STEP_NAME = "retrying-step";

    @Test
    void recoveredImmediateRetryIsParkedAndCancellationPreventsTheNextAttempt() {
        var fixture = fixture();
        try (fixture) {
            fixture.delegate.execute(command());

            assertThat(fixture.runningSteps.stepNames()).containsExactly(STEP_NAME);
            assertThat(fixture.runningSteps.cancelWithCause(
                    STEP_NAME, new WorkflowCancelledException("workflow cancelled")
            )).isTrue();

            fixture.runNextTask();

            verify(fixture.executeDelegate, never()).execute(eq(command()), any(), any());
            assertThat(fixture.runningSteps.stepNames()).isEmpty();
        }
    }

    @Test
    void recoveredDelayedRetryCancelsItsTimerWhenTheParkedStepIsCancelled() {
        var fixture = fixture();
        try (fixture) {
            fixture.delegate.execute(commandWithBackoff());

            assertThat(fixture.runningSteps.cancelWithCause(
                    STEP_NAME, new WorkflowCancelledException("workflow cancelled")
            )).isTrue();
            assertThat(fixture.scheduler.pendingTaskCount()).isZero();

            verify(fixture.executeDelegate, never()).execute(eq(commandWithBackoff()), any(), any());
        }
    }

    @Test
    void recoveredDelayedRetryLaunchesExactlyOneAttemptWhenItsTimerFires() {
        var fixture = fixture();
        try (fixture) {
            fixture.delegate.execute(commandWithBackoff());

            fixture.scheduler.fireNext();
            assertThat(fixture.scheduler.pendingTaskCount()).isZero();
            assertThat(fixture.runningSteps.stepNames()).isEmpty();

            fixture.runNextTask();

            verify(fixture.executeDelegate).execute(any(), any(), any());
        }
    }

    @Test
    void recoveredDelayedRetryCancelsItsTimerWhenTheWorkflowStops() {
        var fixture = fixture();
        try (fixture) {
            fixture.delegate.execute(commandWithBackoff());

            assertThat(fixture.runningSteps.cancelWithCause(
                    STEP_NAME, new StepInterruptedException("workflow stopped")
            )).isTrue();

            assertThat(fixture.scheduler.pendingTaskCount()).isZero();
        }
    }

    private static Fixture fixture() {
        var workflowExecution = mock(WorkflowExecution.class);
        var state = mock(WorkflowState.class);
        var executeDelegate = mock(ExecuteDelegate.class);
        var queuedTasks = new ArrayDeque<Consumer<WorkflowExecution>>();
        var runningSteps = new RunningSteps();
        var executor = Executors.newSingleThreadExecutor();
        var scheduler = new ControllableWorkflowScheduler();

        when(workflowExecution.state()).thenReturn(state);
        when(state.containsStep(STEP_NAME)).thenReturn(true);
        when(state.getStep(STEP_NAME)).thenReturn(retryingStep());
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        doAnswer(invocation -> {
            queuedTasks.add(invocation.getArgument(0));
            return null;
        }).when(workflowExecution).appendTask(any());

        var delegate = new RetryableExecuteDelegate(
                executeDelegate,
                mock(WorkflowContext.class),
                workflowExecution,
                runningSteps,
                new ReachedSteps(),
                DefaultEventNameCustomizer.Builder.defaults(),
                Clock.fixed(NOW, ZoneOffset.UTC),
                scheduler
        );
        return new Fixture(delegate,
                           executeDelegate,
                           workflowExecution,
                           runningSteps,
                           queuedTasks,
                           executor,
                           scheduler);
    }

    private static WorkflowStep retryingStep() {
        var retryInfo = new StepRetryInfo(1, 2, WorkflowError.from(new IllegalStateException("first attempt failed")));
        return WorkflowStep.retrying(STEP_NAME, retryInfo, NOW, null);
    }

    private static ExecutePrimitive.ExecuteCommand command() {
        return command(RetryPolicy.maxRetries(2));
    }

    private static ExecutePrimitive.ExecuteCommand commandWithBackoff() {
        return command(RetryPolicy.maxRetries(2).withBackoff(BackoffStrategy.fixed(Duration.ofSeconds(1))));
    }

    private static ExecutePrimitive.ExecuteCommand command(RetryPolicy retryPolicy) {
        PayloadProcessor action = (context, payload) -> Map.of();
        return new PrimitiveCommands.WorkflowStepResultExecuteCommand(
                STEP_NAME,
                Map.of(),
                action,
                LocalOnlyPayloadReducer.INSTANCE,
                GlobalOnlyPayloadReducer.INSTANCE,
                Duration.ofSeconds(5),
                DefaultEventNameCustomizer.Builder.defaults(),
                retryPolicy
        );
    }

    private record Fixture(RetryableExecuteDelegate delegate,
                           ExecuteDelegate executeDelegate,
                           WorkflowExecution workflowExecution,
                           RunningSteps runningSteps,
                           Queue<Consumer<WorkflowExecution>> queuedTasks,
                           ExecutorService executor,
                           ControllableWorkflowScheduler scheduler) implements AutoCloseable {

        void runNextTask() {
            var task = queuedTasks.remove();
            task.accept(workflowExecution);
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
