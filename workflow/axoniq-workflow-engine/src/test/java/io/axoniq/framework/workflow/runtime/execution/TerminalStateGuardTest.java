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

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.payload.PayloadReducerRegistry;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Verifies that {@code sendStepEvent()} returns a failed future (not a silently completed one) when the workflow or
 * step is already in a terminal state.
 *
 * @author Stefan Dragisic
 */
class TerminalStateGuardTest {

    private WorkflowExecutionOperations workflowExecutionOperations;
    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private EventSourcedWorkflowState workflowState;
    private EventSink eventSink;
    private ProcessingContext processingContext;
    private UnitOfWorkFactory unitOfWorkFactory;
    private Executor executor;
    private EventNameCustomizer eventNameCustomizer = defaults();

    /**
     * Concrete subclass to expose the protected methods for testing.
     */
    private static class TestableStepExecutor extends AbstractStepExecutor {

        TestableStepExecutor(WorkflowExecutionOperations workflowExecutionOperations, WorkflowExecution workflowExecution,
                             EventNameCustomizer parentEventNameCustomizer, Clock clock,
                             UnitOfWorkFactory unitOfWorkFactory, EventSink eventSink, Executor executor) {
            super(workflowExecutionOperations, workflowExecution, new RunningSteps(), new ReachedSteps(), parentEventNameCustomizer,
                  clock, new ControllableWorkflowScheduler());
        }

        CompletableFuture<Void> testCompleted(String stepName, Map<String, @Nullable Object> payload,
                                              EventNameCustomizer eventNameCustomizer) {
            return completed(stepName, payload, eventNameCustomizer);
        }

        CompletableFuture<Void> testFailed(String stepName, Throwable ex,
                                           EventNameCustomizer eventNameCustomizer) {
            return failed(stepName, ex, eventNameCustomizer);
        }

        CompletableFuture<Void> testStarted(String stepName, Map<String, @Nullable Object> payload,
                                            EventNameCustomizer eventNameCustomizer) {
            return started(stepName, payload, eventNameCustomizer);
        }
    }

    private TestableStepExecutor stepExecutor;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        workflowExecutionOperations = mock(WorkflowExecutionOperations.class);
        workflowContext = mock(WorkflowContext.class);
        workflowExecution = mock(WorkflowExecution.class);
        eventSink = mock(EventSink.class);
        processingContext = mock(ProcessingContext.class);
        unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        executor = Runnable::run;

        when(workflowExecution.processingContext()).thenReturn(processingContext);

        UnitOfWork unitOfWork = mock(UnitOfWork.class);
        when(unitOfWorkFactory.create(any(String.class))).thenReturn(unitOfWork);
        when(unitOfWorkFactory.create(any(Function.class))).thenReturn(unitOfWork);
        when(unitOfWork.executeWithResult(any(Function.class)))
                .thenAnswer(invocation -> {
                    Function<ProcessingContext, CompletableFuture<?>> action = invocation.getArgument(0);
                    return action.apply(processingContext);
                });

        when(processingContext.component(EventConverter.class)).thenReturn(
                new DelegatingEventConverter(new JacksonConverter())
        );
        when(processingContext.component(PayloadReducerRegistry.class)).thenReturn(new PayloadReducerRegistry());
        when(workflowExecutionOperations.processingContext()).thenReturn(processingContext);
        when(workflowExecutionOperations.workflowId()).thenReturn("wf-1");
        when(workflowExecutionOperations.workflowPayload()).thenReturn(Map.of());
        when(workflowExecutionOperations.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(eventSink.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        eventNameCustomizer = defaults();
        workflowState = new EventSourcedWorkflowState("wf-1",
                                                      Map.of(),
                                                      VersionedType.of(new QualifiedName("test-workflow"), "0.0.1"),
                                                      workflowContext,
                                                      Map.of());
        when(workflowExecution.state()).thenReturn(workflowState);

        stepExecutor = new TestableStepExecutor(
                workflowExecutionOperations, workflowExecution, eventNameCustomizer,
                Clock.systemUTC(), unitOfWorkFactory, eventSink, executor
        );
    }

    @Test
    void completedReturnsFailedFutureWhenWorkflowIsTerminal() {
        when(workflowExecutionOperations.workflowStatus()).thenReturn(WorkflowStatus.COMPLETED);

        var future = stepExecutor.testCompleted("step-1", Map.of(), eventNameCustomizer);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("terminal state");
    }

    @Test
    void failedReturnsFailedFutureWhenWorkflowIsTerminal() {
        when(workflowExecutionOperations.workflowStatus()).thenReturn(WorkflowStatus.FAILED);

        var future = stepExecutor.testFailed("step-1", new RuntimeException("boom"), eventNameCustomizer);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("terminal state");
    }

    @Test
    void startedReturnsFailedFutureWhenWorkflowIsTerminal() {
        when(workflowExecutionOperations.workflowStatus()).thenReturn(WorkflowStatus.CANCELLED);

        var future = stepExecutor.testStarted("step-1", Map.of(), eventNameCustomizer);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("terminal state");
    }

    @Test
    void completedReturnsFailedFutureWhenStepIsTerminal() {
        when(workflowExecutionOperations.workflowStatus()).thenReturn(WorkflowStatus.STARTED);

        workflowState.evolve(EventMessageUtils.completedStep(workflowExecutionOperations,
                                                             "step-1",
                                                             Map.of(),
                                                             NAME,
                                                             eventNameCustomizer),
                             processingContext);

        var future = stepExecutor.testCompleted("step-1", Map.of(), eventNameCustomizer);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("terminal state");
    }

    @Test
    void failedFutureContainsStepNameInMessage() {
        workflowState.evolve(EventMessageUtils.completedStep(workflowExecutionOperations, "my-step", Map.of(),
                                                             NAME, eventNameCustomizer),
                             processingContext);

        var future = stepExecutor.testCompleted("my-step", Map.of(), eventNameCustomizer);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("my-step");
    }
}
