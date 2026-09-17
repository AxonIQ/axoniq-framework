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

import io.axoniq.framework.workflow.runtime.api.execution.FutureResolutionTimeoutException;
import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class WaitForDelegateTest {

    private final Clock clock = Clock.fixed(Instant.parse("2026-07-08T10:00:00Z"), ZoneOffset.UTC);
    private WorkflowExecutionOperations workflowExecutionOperations;
    private WorkflowExecution workflowExecution;
    private WorkflowState state;
    private UnitOfWorkFactory unitOfWorkFactory;
    private ProcessingContext processingContext;
    private WaitForDelegate delegate;
    private EventWaitConditions eventWaitConditions;
    private AtomicReference<WorkflowStep> startedStep;
    private ControllableWorkflowScheduler workflowScheduler;
    private RunningSteps runningSteps;

    @BeforeEach
    void setUp() {
        workflowExecutionOperations = mock(WorkflowExecutionOperations.class);
        workflowExecution = mock(WorkflowExecution.class);
        state = mock(WorkflowState.class);
        unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        processingContext = mock(ProcessingContext.class);
        startedStep = new AtomicReference<>();
        workflowScheduler = new ControllableWorkflowScheduler();
        eventWaitConditions = spy(new EventWaitConditions());
        Executor executor = Runnable::run;
        EventNameCustomizer customizer = DefaultEventNameCustomizer.Builder.defaults();

        when(workflowExecutionOperations.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(workflowExecutionOperations.workflowId()).thenReturn("wf-123");
        when(workflowExecutionOperations.workflowVersion()).thenReturn("0.0.1");
        when(workflowExecutionOperations.workflowPayload()).thenReturn(Map.of("orderId", "123"));
        when(workflowExecutionOperations.processingContext()).thenReturn(processingContext);
        when(processingContext.component(EventConverter.class)).thenReturn(TestEventConverter.INSTANCE);

        when(workflowExecution.workflowId()).thenReturn("wf-123");
        when(workflowExecution.state()).thenReturn(state);
        when(workflowExecution.processingContext()).thenReturn(processingContext);
        when(workflowExecution.isRunning()).thenReturn(true);
        when(workflowExecution.hasTasks()).thenReturn(false);
        when(state.containsStep("awaitPayment")).thenAnswer(inv -> startedStep.get() != null);
        when(state.getStep("awaitPayment")).thenAnswer(inv -> startedStep.get());

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var task = (java.util.function.Consumer<WorkflowExecution>) invocation.getArgument(0);
            task.accept(workflowExecution);
            return null;
        }).when(workflowExecution).appendTask(any());

        stubUnitOfWorkFactory();
        stubEventPublishing();

        delegate = new WaitForDelegate(
                workflowExecutionOperations,
                workflowExecution,
                runningSteps = new RunningSteps(),
                eventWaitConditions,
                new ReachedSteps(),
                customizer,
                clock,
                workflowScheduler
        );
    }

    @Test
    void startedWaitEventCarriesReplayableWaitDetails() {
        var condition = mock(EventCondition.class);
        when(condition.qualifiedName()).thenReturn(new QualifiedName("io.acme.PaymentConfirmed"));
        when(condition.associations()).thenReturn(Set.of("payload:orderId=123", "metadata:tenantId=eu"));

        delegate.waitForEvent(new PrimitiveCommands.WorkflowStepResultWaitForCommand(
                "awaitPayment",
                condition,
                GlobalOnlyPayloadReducer.INSTANCE,
                Duration.ofMinutes(15),
                DefaultEventNameCustomizer.Builder.defaults()
        ));

        ArgumentCaptor<EventMessage> eventCaptor = ArgumentCaptor.forClass(EventMessage.class);
        verify(workflowExecution).appendWorkflowEvent(eventCaptor.capture(), any(ProcessingContext.class));

        assertThat(eventCaptor.getValue().payload()).isEqualTo(Map.of(
                "startTime", Instant.parse("2026-07-08T10:00:00Z"),
                "eventName", "io.acme.PaymentConfirmed",
                "associations", Set.of("payload:orderId=123", "metadata:tenantId=eu"),
                "timeoutTime", Instant.parse("2026-07-08T10:15:00Z")
        ));
        verify(eventWaitConditions).add(anyString(), any(EventCondition.class), any(), any());
    }

    @Test
    void cancellingAParkedWaitCancelsItsTimer() {
        var condition = mock(EventCondition.class);
        when(condition.qualifiedName()).thenReturn(new QualifiedName("io.acme.PaymentConfirmed"));
        when(condition.associations()).thenReturn(Set.of());

        delegate.waitForEvent(new PrimitiveCommands.WorkflowStepResultWaitForCommand(
                "awaitPayment", condition, GlobalOnlyPayloadReducer.INSTANCE, Duration.ofMinutes(15),
                DefaultEventNameCustomizer.Builder.defaults()
        ));

        assertThat(runningSteps.cancelWithCause("awaitPayment", new StepCancellationException("cancelled"))).isTrue();

        assertThat(workflowScheduler.pendingTaskCount()).isZero();
    }

    @Test
    void receivingTheAwaitedEventCancelsItsTimer() {
        var condition = mock(EventCondition.class);
        when(condition.qualifiedName()).thenReturn(new QualifiedName("io.acme.PaymentConfirmed"));
        when(condition.associations()).thenReturn(Set.of());
        var event = mock(EventMessage.class);
        when(event.payloadAs(any(org.axonframework.common.TypeReference.class))).thenReturn(Map.of());

        delegate.waitForEvent(new PrimitiveCommands.WorkflowStepResultWaitForCommand(
                "awaitPayment", condition, GlobalOnlyPayloadReducer.INSTANCE, Duration.ofMinutes(15),
                DefaultEventNameCustomizer.Builder.defaults()
        ));

        delegate.eventReceived(new EventWaitConditions.Awaited(
                event, processingContext, "awaitPayment", GlobalOnlyPayloadReducer.INSTANCE,
                DefaultEventNameCustomizer.Builder.defaults()
        ));

        assertThat(workflowScheduler.pendingTaskCount()).isZero();
    }

    @Test
    void receivingTheAwaitedEventPropagatesPublicationResolutionTimeout() {
        var event = mock(EventMessage.class);
        when(event.payloadAs(any(org.axonframework.common.TypeReference.class))).thenReturn(Map.of());
        when(processingContext.component(io.axoniq.framework.workflow.runtime.util.FutureResolver.class))
                .thenReturn(ignored -> {
                    throw new FutureResolutionTimeoutException(new TimeoutException("publication timed out"));
                });

        assertThatThrownBy(() -> delegate.eventReceived(new EventWaitConditions.Awaited(
                event, processingContext, "awaitPayment", GlobalOnlyPayloadReducer.INSTANCE,
                DefaultEventNameCustomizer.Builder.defaults()
        ))).isInstanceOf(FutureResolutionTimeoutException.class);
    }

    @Test
    void anAlreadyCompletedTimerIsNotLeftRegisteredAsRunning() {
        workflowScheduler.fireDuringSchedule();
        var condition = mock(EventCondition.class);
        when(condition.qualifiedName()).thenReturn(new QualifiedName("io.acme.PaymentConfirmed"));
        when(condition.associations()).thenReturn(Set.of());

        delegate.waitForEvent(new PrimitiveCommands.WorkflowStepResultWaitForCommand(
                "awaitPayment", condition, GlobalOnlyPayloadReducer.INSTANCE, Duration.ofMinutes(15),
                DefaultEventNameCustomizer.Builder.defaults()
        ));

        assertThat(runningSteps.stepNames()).isEmpty();
        assertThat(workflowScheduler.pendingTaskCount()).isZero();
    }

    private void stubUnitOfWorkFactory() {
        UnitOfWork unitOfWork = mock(UnitOfWork.class);
        when(unitOfWorkFactory.create(anyString(), any(Function.class))).thenReturn(unitOfWork);
        when(unitOfWork.executeWithResult(any(Function.class))).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Function<ProcessingContext, CompletableFuture<Void>> action = invocation.getArgument(0);
            return action.apply(processingContext);
        });
        when(processingContext.resources()).thenReturn(Map.of());
    }

    private void stubEventPublishing() {
        when(workflowExecution.appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class)))
                .thenAnswer(invocation -> {
            EventMessage message = invocation.getArgument(0);
            if (message.payload() instanceof Map<?, ?> payload) {
                @SuppressWarnings("unchecked")
                Map<String, @Nullable Object> startPayload = (Map<String, @Nullable Object>) payload;
                startedStep.set(WorkflowStep.started(
                        "awaitPayment",
                        startPayload,
                        (Instant) startPayload.get("startTime"),
                        processingContext
                ));
            }
                    return CompletableFuture.completedFuture(null);
                });
    }
}
