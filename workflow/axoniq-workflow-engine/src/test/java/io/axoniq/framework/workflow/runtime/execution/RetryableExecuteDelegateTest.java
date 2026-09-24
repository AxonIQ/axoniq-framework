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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.PayloadProcessor;
import io.axoniq.framework.workflow.dsl.api.StepIndeterminateException;
import io.axoniq.framework.workflow.dsl.api.StepInterruptedException;
import io.axoniq.framework.workflow.dsl.api.StepRetryInfo;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowCancelledException;
import io.axoniq.framework.workflow.dsl.api.WorkflowError;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStep;
import io.axoniq.framework.workflow.dsl.api.retry.BackoffStrategy;
import io.axoniq.framework.workflow.dsl.api.retry.RetryPolicy;
import io.axoniq.framework.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
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

    private static Fixture fixture() {
        return fixture(retryingStep());
    }

    private static Fixture fixture(WorkflowStep recoveredStep) {
        var workflowExecutionOperations = mock(WorkflowExecutionOperations.class);
        var workflowExecution = mock(WorkflowExecution.class);
        var state = mock(WorkflowState.class);
        var executeDelegate = mock(ExecuteDelegate.class);
        var queuedTasks = new ArrayDeque<Consumer<WorkflowExecution>>();
        var publishedEvents = new ArrayList<EventMessage>();
        var runningSteps = new RunningSteps();
        var executor = Executors.newSingleThreadExecutor();
        var scheduler = new ControllableWorkflowScheduler();

        var processingContext = mock(ProcessingContext.class);
        when(processingContext.component(EventConverter.class)).thenReturn(mock(EventConverter.class));
        when(workflowExecutionOperations.processingContext()).thenReturn(processingContext);
        when(workflowExecutionOperations.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(workflowExecution.state()).thenReturn(state);
        when(state.containsStep(STEP_NAME)).thenReturn(true);
        when(state.getStep(STEP_NAME)).thenReturn(recoveredStep);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        doAnswer(invocation -> {
            queuedTasks.add(invocation.getArgument(0));
            return null;
        }).when(workflowExecution).appendTask(any());
        when(workflowExecution.appendWorkflowEvent(any(), any())).thenAnswer(invocation -> {
            publishedEvents.add(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });

        var delegate = new RetryableExecuteDelegate(
                executeDelegate,
                workflowExecutionOperations,
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
                           publishedEvents,
                           executor,
                           scheduler);
    }

    private static WorkflowStep retryingStep() {
        var retryInfo = new StepRetryInfo(1, 2, WorkflowError.from(new IllegalStateException("first attempt failed")));
        return WorkflowStep.retrying(STEP_NAME, retryInfo, NOW, null);
    }

    private static WorkflowStep retryStartedStep(int attempt) {
        var retryInfo = new StepRetryInfo(attempt, 2, WorkflowError.from(new IllegalStateException("attempt failed")));
        return WorkflowStep.retryStarted(STEP_NAME, retryInfo, NOW, null);
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

    @Test
    void recoveredInFlightRetryAttemptKeepsItsAttemptNumber() {
        // given the log ends with RETRY_STARTED for attempt 2: attempt 2 was running when the engine stopped
        var fixture = fixture(retryStartedStep(2));
        try (fixture) {
            fixture.delegate.execute(command());

            var failureHandler = ArgumentCaptor.forClass(FailureHandler.class);
            verify(fixture.executeDelegate).execute(any(), failureHandler.capture(), any());

            // when the delegate reports the resumed attempt as indeterminate
            failureHandler.getValue().onFailure(STEP_NAME,
                                                new StepIndeterminateException(STEP_NAME),
                                                DefaultEventNameCustomizer.Builder.defaults());
            fixture.runNextTask();

            // then RETRYING is recorded for attempt 2, not for attempt 1
            assertThat(fixture.publishedEvents).singleElement().satisfies(event -> {
                assertThat(MetadataUtils.getStepStatus(event.metadata())).contains(StepStatus.RETRYING);
                assertThat(event.payloadAs(StepRetryInfo.class).attempt()).isEqualTo(2);
            });
        }
    }

    private record Fixture(RetryableExecuteDelegate delegate,
                           ExecuteDelegate executeDelegate,
                           WorkflowExecution workflowExecution,
                           RunningSteps runningSteps,
                           Queue<Consumer<WorkflowExecution>> queuedTasks,
                           List<EventMessage> publishedEvents,
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
