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

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WaitForDelegateTest {

    private final Clock clock = Clock.fixed(Instant.parse("2026-07-08T10:00:00Z"), ZoneOffset.UTC);
    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private WorkflowState state;
    private UnitOfWorkFactory unitOfWorkFactory;
    private EventSink eventSink;
    private ProcessingContext processingContext;
    private WaitForDelegate delegate;
    private AtomicReference<WorkflowStep> startedStep;

    @BeforeEach
    void setUp() {
        workflowContext = mock(WorkflowContext.class);
        workflowExecution = mock(WorkflowExecution.class);
        state = mock(WorkflowState.class);
        unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        eventSink = mock(EventSink.class);
        processingContext = mock(ProcessingContext.class);
        startedStep = new AtomicReference<>();
        Executor executor = Runnable::run;
        EventNameCustomizer customizer = DefaultEventNameCustomizer.Builder.defaults();

        when(workflowContext.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(workflowContext.workflowId()).thenReturn("wf-123");
        when(workflowContext.workflowVersion()).thenReturn("0.0.1");
        when(workflowContext.workflowPayload()).thenReturn(Map.of("orderId", "123"));

        when(workflowExecution.workflowId()).thenReturn("wf-123");
        when(workflowExecution.state()).thenReturn(state);
        when(workflowExecution.processingContext()).thenReturn(processingContext);
        when(workflowExecution.isExecutable()).thenReturn(true);
        when(workflowExecution.hasTasks()).thenReturn(true);
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
                workflowContext,
                workflowExecution,
                customizer,
                clock,
                unitOfWorkFactory,
                eventSink,
                executor
        );
    }

    @Test
    void startedWaitEventCarriesReplayableWaitDetails() {
        var condition = mock(EventCondition.class);
        when(condition.qualifiedName()).thenReturn(new QualifiedName("io.acme.PaymentConfirmed"));
        when(condition.serializedAssociations()).thenReturn(Set.of("payload:orderId=123", "metadata:tenantId=eu"));

        delegate.waitForEvent(new PrimitiveCommands.WorkflowStepResultWaitForCommand(
                "awaitPayment",
                condition,
                GlobalOnlyPayloadReducer.INSTANCE,
                Duration.ofMinutes(15),
                DefaultEventNameCustomizer.Builder.defaults()
        ));

        ArgumentCaptor<EventMessage> eventCaptor = ArgumentCaptor.forClass(EventMessage.class);
        verify(eventSink).publish(any(ProcessingContext.class), eventCaptor.capture());

        assertThat(eventCaptor.getValue().payload()).isEqualTo(Map.of(
                "startTime", Instant.parse("2026-07-08T10:00:00Z"),
                "eventQualifiedName", "io.acme.PaymentConfirmed",
                "serializedAssociations", "payload:orderId=123;metadata:tenantId=eu",
                "timeoutTime", Instant.parse("2026-07-08T10:15:00Z")
        ));
        assertThat(eventCaptor.getValue().metadata().get("stepPrimitive")).isEqualTo("WAIT_FOR_EVENT");
        verify(workflowExecution).registerWaitCondition(anyString(), any(EventCondition.class), any(), any());
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
        when(eventSink.publish(any(ProcessingContext.class), any(EventMessage.class))).thenAnswer(invocation -> {
            EventMessage message = invocation.getArgument(1);
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) message.payload();
            startedStep.set(WorkflowStep.started(
                    "awaitPayment",
                    payload,
                    (Instant) payload.get("startTime"),
                    processingContext
            ));
            return CompletableFuture.completedFuture(null);
        });
    }
}
