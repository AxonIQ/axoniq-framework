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

import org.jspecify.annotations.Nullable;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadModification;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;

/**
 * Test for {@link PayloadDelegate}.
 *
 * @author Simon Zambrovski
 */
class PayloadDelegateTest {

    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private EventSink eventSink;
    private ProcessingContext processingContext;
    private UnitOfWorkFactory unitOfWorkFactory;
    private Executor executor;
    private EventNameCustomizer parentEventNameCustomizer;
    private Clock clock;
    private PayloadDelegate delegate;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        workflowContext = mock(WorkflowContext.class);
        workflowExecution = mock(WorkflowExecution.class);
        eventSink = mock(EventSink.class);
        processingContext = mock(ProcessingContext.class);
        unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        executor = Runnable::run;
        clock = Clock.systemUTC();
        parentEventNameCustomizer = DefaultEventNameCustomizer.Builder.defaults();

        UnitOfWork unitOfWork = mock(UnitOfWork.class);
        when(unitOfWorkFactory.create(any(Function.class))).thenReturn(unitOfWork);
        when(unitOfWorkFactory.create(anyString(), any(Function.class))).thenReturn(unitOfWork);
        when(unitOfWork.executeWithResult(any(Function.class)))
                .thenAnswer(invocation -> {
                    Function<ProcessingContext, CompletableFuture<?>> action = invocation.getArgument(0);
                    return action.apply(processingContext);
                });

        when(workflowExecution.processingContext()).thenReturn(processingContext);
        when(workflowExecution.workflowContext()).thenReturn(workflowContext);
        when(workflowExecution.state()).thenReturn(mock(WorkflowState.class));
        when(workflowContext.processingContext()).thenReturn(processingContext);
        when(workflowContext.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(processingContext.component(EventConverter.class)).thenReturn(TestEventConverter.INSTANCE);
        when(eventSink.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        delegate = new PayloadDelegate(
                workflowContext,
                workflowExecution,
                new RunningSteps(),
                new ReachedSteps(),
                parentEventNameCustomizer,
                clock,
                unitOfWorkFactory,
                eventSink,
                executor,
                new ControllableWorkflowScheduler()
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void modifyPayloadAppendsTaskAndPublishesEvent() {
        String stepName = "testStep";
        Map<String, @Nullable Object> currentPayload = new HashMap<>();
        currentPayload.put("key1", "value1");
        when(workflowContext.workflowPayload()).thenReturn(currentPayload);

        PayloadModification modification = p -> {
            Map<String, @Nullable Object> next = new HashMap<>(p);
            next.put("key2", "value2");
            return next;
        };
        EventNameCustomizer customizer = DefaultEventNameCustomizer.Builder.defaults();

        // Capture the task appended to workflow execution
        ArgumentCaptor<Consumer<WorkflowExecution>> taskCaptor = ArgumentCaptor.forClass(Consumer.class);
        WorkflowStepResult result = delegate.modifyPayload(
                PrimitiveCommands.modifyPayload(stepName, modification, customizer)
        );

        verify(workflowExecution).appendTask(taskCaptor.capture());
        Consumer<WorkflowExecution> task = taskCaptor.getValue();
        assertThat(result.getStepName()).isEqualTo(stepName);

        // Execute the task
        task.accept(workflowExecution);

        // Verify event publishing
        ArgumentCaptor<EventMessage> eventCaptor = ArgumentCaptor.forClass(EventMessage.class);
        verify(eventSink).publish(eq(processingContext), eventCaptor.capture());

        EventMessage event = eventCaptor.getValue();
        Map<String, @Nullable Object> eventPayload = (Map<String, @Nullable Object>) event.payload();
        assertThat(eventPayload).containsEntry("key1", "value1");
        assertThat(eventPayload).containsEntry("key2", "value2");
    }

    @Test
    void modifyPayloadWaitsForStepCompletion() throws InterruptedException {
        String stepName = "testStep";
        PayloadModification modification = p -> p;
        EventNameCustomizer customizer = DefaultEventNameCustomizer.Builder.defaults();

        delegate.modifyPayload(PrimitiveCommands.modifyPayload(stepName, modification, customizer));

        ArgumentCaptor<Predicate<WorkflowState>> predicateCaptor = ArgumentCaptor.forClass(Predicate.class);
        verify(workflowExecution).awaitStateChange(predicateCaptor.capture());
        Predicate<WorkflowState> predicate = predicateCaptor.getValue();

        // Test the predicate
        WorkflowState state = mock(WorkflowState.class);
        WorkflowStep stepState = mock(WorkflowStep.class);

        when(state.containsStep(stepName)).thenReturn(false);
        assertThat(predicate.test(state)).isFalse();

        when(state.containsStep(stepName)).thenReturn(true);
        when(state.getStep(stepName)).thenReturn(stepState);
        when(stepState.status()).thenReturn(StepStatus.STARTED);
        assertThat(predicate.test(state)).isFalse();

        when(stepState.status()).thenReturn(StepStatus.COMPLETED);
        assertThat(predicate.test(state)).isTrue();
    }
}
