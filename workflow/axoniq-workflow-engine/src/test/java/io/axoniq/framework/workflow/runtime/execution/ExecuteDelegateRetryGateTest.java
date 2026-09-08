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

import io.axoniq.framework.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepIndeterminateException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests that a retry attempt of an execute step passes the same accepted-append gate as the first attempt: its action
 * runs only after the store accepted this execution's own {@code RETRY_STARTED} record.
 *
 * @author Stefan Dragisic
 */
class ExecuteDelegateRetryGateTest {

    private static final String STEP_NAME = "ship";
    private static final StepRetryInfo FIRST_ATTEMPT_FAILED =
            new StepRetryInfo(1, 2, WorkflowError.from(new IllegalStateException("first attempt failed")));

    private WorkflowExecution workflowExecution;
    private UnitOfWork unitOfWork;
    private ExecuteDelegate delegate;
    private final AtomicReference<WorkflowStep> currentStep = new AtomicReference<>();
    private final List<EventMessage> acceptedEvents = new ArrayList<>();
    private final List<Throwable> reportedFailures = new ArrayList<>();
    private boolean storeAcceptsAppends = true;

    @BeforeEach
    void setUp() throws InterruptedException {
        var workflowContext = mock(WorkflowContext.class);
        workflowExecution = mock(WorkflowExecution.class);
        var workflowState = mock(WorkflowState.class);
        var unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        unitOfWork = mock(UnitOfWork.class);
        var scheduler = mock(WorkflowScheduler.class);
        var timeoutTask = mock(WorkflowScheduler.ScheduledTask.class);
        var queuedTasks = new ArrayDeque<Consumer<WorkflowExecution>>();

        var processingContext = mock(ProcessingContext.class);
        when(processingContext.component(EventConverter.class)).thenReturn(mock(EventConverter.class));
        when(workflowContext.processingContext()).thenReturn(processingContext);
        when(workflowContext.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(workflowExecution.workflowId()).thenReturn("wf-1");
        when(workflowExecution.state()).thenReturn(workflowState);
        when(workflowExecution.isRunning()).thenReturn(true);
        when(workflowExecution.processingContext()).thenReturn(mock(ProcessingContext.class));
        when(workflowState.containsStep(STEP_NAME)).thenAnswer(invocation -> currentStep.get() != null);
        when(workflowState.getStep(STEP_NAME)).thenAnswer(invocation -> currentStep.get());
        when(unitOfWorkFactory.create(anyString(), any())).thenReturn(unitOfWork);
        when(unitOfWork.executeWithResult(any())).thenReturn(new CompletableFuture<>());
        when(scheduler.schedule(any())).thenReturn(timeoutTask);
        when(timeoutTask.completion()).thenReturn(new CompletableFuture<>());
        doAnswer(invocation -> {
            queuedTasks.add(invocation.getArgument(0));
            return null;
        }).when(workflowExecution).appendTask(any());
        // The driver applies queued tasks while a step waits for a state change.
        doAnswer(invocation -> {
            while (!queuedTasks.isEmpty()) {
                queuedTasks.remove().accept(workflowExecution);
            }
            return null;
        }).when(workflowExecution).awaitStateChange(any());
        // The store either accepts the append and evolves the step, or rejects it and leaves the state alone.
        when(workflowExecution.appendWorkflowEvent(any(), any())).thenAnswer(invocation -> {
            EventMessage event = invocation.getArgument(0);
            if (!storeAcceptsAppends) {
                return CompletableFuture.failedFuture(new IllegalStateException("another writer owns the instance"));
            }
            acceptedEvents.add(event);
            if (MetadataUtils.getStepStatus(event.metadata()).orElseThrow() == StepStatus.RETRY_STARTED) {
                currentStep.set(WorkflowStep.retryStarted(STEP_NAME, event.payloadAs(StepRetryInfo.class),
                                                          Instant.now(), null));
            }
            return CompletableFuture.completedFuture(null);
        });

        delegate = new ExecuteDelegate(
                workflowContext,
                workflowExecution,
                new RunningSteps(),
                new ReachedSteps(),
                DefaultEventNameCustomizer.Builder.defaults(),
                Clock.systemUTC(),
                unitOfWorkFactory,
                Runnable::run,
                scheduler,
                new DefaultExecuteStepActionResolver()
        );
    }

    @Nested
    class RetryAttempt {

        @Test
        void publishesRetryStartedForTheNextAttemptBeforeRunningTheAction() {
            // given a step whose first attempt failed and whose backoff has elapsed
            currentStep.set(WorkflowStep.retrying(STEP_NAME, FIRST_ATTEMPT_FAILED, Instant.now(), null));

            // when the retry attempt is launched
            execute();

            // then the store accepted RETRY_STARTED for attempt 2 and only then did the action start
            assertThat(acceptedEvents).singleElement().satisfies(event -> {
                assertThat(MetadataUtils.getStepStatus(event.metadata())).contains(StepStatus.RETRY_STARTED);
                assertThat(event.payloadAs(StepRetryInfo.class).attempt()).isEqualTo(2);
                assertThat(event.payloadAs(StepRetryInfo.class).maxRetries()).isEqualTo(2);
            });
            assertThat(currentStep.get().status()).isEqualTo(StepStatus.RETRY_STARTED);
            verify(unitOfWork).executeWithResult(any());
            assertThat(reportedFailures).isEmpty();
        }

        @Test
        void rejectedRetryStartedCancelsTheAttemptWithoutRunningTheAction() {
            // given a step in backoff on a node that lost the instance: the store rejects its appends
            currentStep.set(WorkflowStep.retrying(STEP_NAME, FIRST_ATTEMPT_FAILED, Instant.now(), null));
            storeAcceptsAppends = false;

            // when the retry attempt is launched
            var result = execute();

            // then nothing was recorded, the action never ran and the step is left to the owner
            assertThat(acceptedEvents).isEmpty();
            assertThat(result.canceled()).isTrue();
            verify(unitOfWork, never()).executeWithResult(any());
            assertThat(reportedFailures).isEmpty();
        }

        @Test
        void foreignRetryStartedAtEntryIsReportedAsIndeterminateWithoutRunningTheAction() {
            // given another execution already started attempt 2 of this step
            var attemptTwo = new StepRetryInfo(2, 2, FIRST_ATTEMPT_FAILED.error());
            currentStep.set(WorkflowStep.retryStarted(STEP_NAME, attemptTwo, Instant.now(), null));

            // when this execution reaches the step
            execute();

            // then the attempt is routed through the failure handler and the action does not run here
            assertThat(reportedFailures).singleElement().isInstanceOf(StepIndeterminateException.class);
            assertThat(acceptedEvents).isEmpty();
            verify(unitOfWork, never()).executeWithResult(any());
        }
    }

    private WorkflowStepResult execute() {
        return delegate.execute(command(),
                                (name, error, customizer) -> reportedFailures.add(error),
                                (name, customizer) -> {
                                    // timeouts are not part of these tests
                                });
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
                RetryPolicy.maxRetries(2)
        );
    }
}
