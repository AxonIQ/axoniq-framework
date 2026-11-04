/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests that a {@link SimpleWorkflowExecution} restored from durable state replays its completed payload and publish
 * steps without waiting for an event. A segment claim restores an instance with an empty task queue, so a replayed step
 * that waits for a task parks the body until some unrelated event happens to reach the instance.
 *
 * @author Stefan Dragisic
 */
class SimpleWorkflowExecutionRestoredReplayTest {

    private static final String WORKFLOW_ID = "workflow-id";

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicReference<SimpleWorkflowExecution> target = new AtomicReference<>();

    @AfterEach
    void shutDownExecutor() {
        executor.shutdownNow();
    }

    @Test
    void completedPayloadStepReplaysWithoutWaitingForAnEvent() throws Exception {
        // given: a restored instance whose payload step completed before the node stopped
        var execution = restoredExecution("initializeTransfer", ignored -> target.get()
                .workflowExecutionOperations()
                .modifyPayload(PrimitiveCommands.modifyPayload("initializeTransfer", p -> p, defaults())));

        // when: the body replays with no event delivered to the instance
        var body = execution.execute(ignored -> {
        });

        // then
        body.get(5, TimeUnit.SECONDS);
        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
    }

    @Test
    void completedPublishStepReplaysWithoutWaitingForAnEvent() throws Exception {
        // given: a restored instance whose publish step completed before the node stopped
        var execution = restoredExecution("recordWithdrawal", ignored -> target.get()
                .workflowExecutionOperations()
                .publish(PrimitiveCommands.publish("recordWithdrawal", event("TransferWithdrawn", Map.of()))));

        // when: the body replays with no event delivered to the instance
        var body = execution.execute(ignored -> {
        });

        // then
        body.get(5, TimeUnit.SECONDS);
        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
    }

    /**
     * Builds an execution the way a segment claim restores it: from durable state that is started and holds the given
     * step as completed, with an empty task queue.
     */
    @SuppressWarnings("unchecked")
    private SimpleWorkflowExecution restoredExecution(String completedStep, WorkflowDefinition<WorkflowContext> body) {
        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        when(configuration.workflowName()).thenReturn("test-workflow");
        when(configuration.workflowVersion()).thenReturn("1.0.0");
        when(configuration.eventNameCustomizer()).thenReturn(defaults());
        when(configuration.workflowStatusChangeListeners()).thenReturn(Map.of());
        when(configuration.workflowDefinition()).thenReturn(body);

        var execution = new SimpleWorkflowExecution(
                WORKFLOW_ID, Map.of(), processingContext(), configuration, mock(WorkflowContext.class)
        );
        target.set(execution);

        var definition = execution.state().workflowDefinitionId();
        var state = new EventSourcedWorkflowState(WORKFLOW_ID, Map.of(), definition);
        state.evolve(event("test-workflow", MetadataUtils.create(WORKFLOW_ID, WorkflowStatus.STARTED, definition)),
                     execution.processingContext(),
                     false);
        state.evolve(event(completedStep, MetadataUtils.create(WORKFLOW_ID, completedStep, StepStatus.COMPLETED)),
                     execution.processingContext(),
                     false);
        execution.initializeState(state);
        return execution;
    }

    private static EventMessage event(String name, Map<String, String> metadata) {
        return new GenericEventMessage(new MessageType(new QualifiedName(name)), Map.of(), metadata)
                .withConverter(TestEventConverter.INSTANCE);
    }

    private ProcessingContext processingContext() {
        var eventStore = echoingEventStore();
        var processingContext = mock(ProcessingContext.class);
        when(processingContext.resources()).thenReturn(Map.of());
        when(processingContext.component(UnitOfWorkFactory.class))
                .thenReturn(new SimpleUnitOfWorkFactory(processingContext));
        when(processingContext.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(processingContext.component(EventStore.class)).thenReturn(eventStore);
        when(processingContext.component(EventConverter.class)).thenReturn(TestEventConverter.INSTANCE);
        when(processingContext.component(WorkflowScheduler.class)).thenReturn(new ControllableWorkflowScheduler());
        when(processingContext.component(ExecuteStepActionResolver.class))
                .thenReturn(new DefaultExecuteStepActionResolver());
        when(processingContext.component(eq(ExecutorService.class), any())).thenReturn(executor);
        when(processingContext.component(eq(java.util.concurrent.Executor.class), any())).thenReturn(executor);
        return processingContext;
    }

    /**
     * Returns an event store that feeds every appended event straight back into the execution, the way the processor
     * would deliver it, so the terminal transition at the end of the body can complete.
     */
    private EventStore echoingEventStore() {
        var eventStore = mock(EventStore.class);
        when(eventStore.transaction(any(ProcessingContext.class))).thenReturn(mock(EventStoreTransaction.class));
        when(eventStore.publish(any(ProcessingContext.class), any(EventMessage.class))).thenAnswer(invocation -> {
            var execution = target.get();
            execution.onEvent(invocation.getArgument(1), execution.processingContext());
            return CompletableFuture.completedFuture(null);
        });
        return eventStore;
    }
}
