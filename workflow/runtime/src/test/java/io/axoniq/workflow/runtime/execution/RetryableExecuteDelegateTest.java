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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.Test;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;

/**
 * Tests retry-attempt scheduling independently from workflow integration tests.
 *
 * @author Simon Zambrovski
 */
class RetryableExecuteDelegateTest {

    private static final Instant NOW = Instant.parse("2026-08-18T10:00:00Z");
    private static final String STEP_NAME = "retrying-step";

    @Test
    void recoveredImmediateRetry_isParkedAndCancellationPreventsTheNextAttempt() {
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

    private static Fixture fixture() {
        var workflowExecution = mock(WorkflowExecution.class);
        var state = mock(WorkflowState.class);
        var executeDelegate = mock(ExecuteDelegate.class);
        var queuedTasks = new ArrayDeque<Consumer<WorkflowExecution>>();
        var runningSteps = new RunningSteps();
        var executor = Executors.newSingleThreadExecutor();

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
                mock(UnitOfWorkFactory.class),
                mock(EventSink.class),
                executor,
                mock(WorkflowScheduler.class)
        );
        return new Fixture(delegate, executeDelegate, workflowExecution, runningSteps, queuedTasks, executor);
    }

    private static WorkflowStep retryingStep() {
        var retryInfo = new StepRetryInfo(1, 2, WorkflowError.from(new IllegalStateException("first attempt failed")));
        return WorkflowStep.retrying(STEP_NAME, retryInfo, NOW, null);
    }

    private static ExecutePrimitive.ExecuteCommand command() {
        PayloadProcessor action = (context, payload) -> Map.of();
        return new PrimitiveCommands.WorkflowStepResultExecuteCommand(
                STEP_NAME,
                Map.of(),
                action,
                LocalOnlyPayloadReducer.INSTANCE,
                GlobalOnlyPayloadReducer.INSTANCE,
                Duration.ofSeconds(5),
                DefaultEventNameCustomizer.Builder.defaults(),
                RetryPolicy.maxRetries(2)
        );
    }

    private record Fixture(RetryableExecuteDelegate delegate,
                           ExecuteDelegate executeDelegate,
                           WorkflowExecution workflowExecution,
                           RunningSteps runningSteps,
                           Queue<Consumer<WorkflowExecution>> queuedTasks,
                           ExecutorService executor) implements AutoCloseable {

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
