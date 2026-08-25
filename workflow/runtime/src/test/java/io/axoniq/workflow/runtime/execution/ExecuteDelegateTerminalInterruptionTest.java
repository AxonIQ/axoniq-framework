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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.StepInterruptedException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests that a whole-workflow terminal interrupt does not create a durable step failure.
 *
 * @author Simon Zambrovski
 */
class ExecuteDelegateTerminalInterruptionTest {

    private static final String STEP_NAME = "running-step";

    @Test
    void terminalInterruptionDoesNotAppendFailureTask() {
        var workflowExecution = mock(WorkflowExecution.class);
        var workflowState = mock(WorkflowState.class);
        var runningSteps = new RunningSteps();
        var queuedTasks = new ArrayDeque<Consumer<WorkflowExecution>>();
        var pendingAction = new CompletableFuture<Object>();
        var unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        var unitOfWork = mock(UnitOfWork.class);
        var scheduler = mock(WorkflowScheduler.class);
        var timeoutTask = mock(WorkflowScheduler.ScheduledTask.class);

        when(workflowExecution.workflowId()).thenReturn("wf-1");
        when(workflowExecution.state()).thenReturn(workflowState);
        when(workflowExecution.isRunning()).thenReturn(true);
        when(workflowState.containsStep(STEP_NAME)).thenReturn(true);
        when(workflowState.getStep(STEP_NAME)).thenReturn(retryingStep());
        when(unitOfWorkFactory.create(anyString(), any())).thenReturn(unitOfWork);
        when(unitOfWork.executeWithResult(any())).thenReturn(pendingAction);
        when(scheduler.schedule(any(), any())).thenReturn(timeoutTask);
        doAnswer(invocation -> {
            queuedTasks.add(invocation.getArgument(0));
            return null;
        }).when(workflowExecution).appendTask(any());

        var delegate = new ExecuteDelegate(
                mock(WorkflowContext.class),
                workflowExecution,
                runningSteps,
                new ReachedSteps(),
                DefaultEventNameCustomizer.Builder.defaults(),
                Clock.systemUTC(),
                unitOfWorkFactory,
                mock(EventSink.class),
                Runnable::run,
                scheduler,
                new DefaultExecuteStepActionResolver()
        );

        delegate.execute(command());
        assertThat(pendingAction).isNotDone();
        assertThat(queuedTasks).isEmpty();
        runningSteps.cancelAll(new StepInterruptedException("Workflow reached terminal state"), ignored -> {
            // nothing to clean up
        });

        assertThat(queuedTasks).isEmpty();
    }

    private static WorkflowStep retryingStep() {
        return new WorkflowStep(STEP_NAME, StepStatus.RETRYING, null, null, Instant.now(), null);
    }

    private static ExecutePrimitive.ExecuteCommand command() {
        PayloadProcessor action = (context, payload) -> Map.of();
        return new PrimitiveCommands.WorkflowStepResultExecuteCommand(
                STEP_NAME,
                Map.of(),
                action,
                LocalOnlyPayloadReducer.INSTANCE,
                GlobalOnlyPayloadReducer.INSTANCE,
                Duration.ofMinutes(1),
                DefaultEventNameCustomizer.Builder.defaults(),
                RetryPolicy.NONE
        );
    }
}
