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

import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static io.axoniq.workflow.configuration.WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.owningSegment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Claiming a segment starts the bodies of the instances it restores <em>asynchronously</em> and returns. The processor
 * is then free to deliver that segment's catch-up backlog straight away, into a window in which a restored instance is
 * already marked running but has not yet replayed back to its {@code awaitEvent} call, so it holds no wait condition.
 * <p>
 * An event delivered in that window must still wake the instance. The engine's own task queue is the instance's single
 * ordered timeline, so an event queued on it is matched against the conditions the body holds by the time the body
 * drains the queue, rather than against the conditions it happened to hold on the delivering thread.
 * <p>
 * The rig reproduces the window deterministically: the workflow executor never runs the submitted body, which is
 * exactly the state {@code restoreWorkflowsFor} leaves behind when it returns, and the test then plays the two
 * orderings by hand.
 */
class WorkflowEngineClaimGapWakeTest {

    private static final String RESIDENT_ID = "sharded-0";
    private static final MessageType DEFINITION_ID =
            new MessageType(new QualifiedName("RestoredWorkflow"), "1.0.0");
    private static final QualifiedName RESUME_EVENT = new QualifiedName("io.axoniq.test", "PaymentReceived");
    private static final String WAIT_STEP = "awaitPayment";

    private WorkflowConfigurationRegistry<?> configurationRegistry;
    private WorkflowStore workflowStore;
    private WorkflowEngine workflowEngine;
    private WorkflowEngineReplaySupport replaySupport;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;

    /**
     * The real execution the claim materializes, so the wake travels the production path.
     */
    private SimpleWorkflowExecution restored;

    @BeforeEach
    void setUp() {
        configurationRegistry = mock(WorkflowConfigurationRegistry.class);
        workflowStore = mock(WorkflowStore.class);
        workflowEngine = new WorkflowEngine(
                configurationRegistry,
                new InMemoryWorkflowExecutionRepository(),
                mock(WorkflowCancellationService.class),
                workflowStore,
                mock(UnitOfWorkFactory.class)
        );
        replaySupport = new WorkflowEngineReplaySupport(workflowEngine);
        checkpointingSupport = new WorkflowEngineCheckpointingSupport(workflowEngine);
        workflowEngine.setEngineSupportComponents(replaySupport, checkpointingSupport);
        when(configurationRegistry.getHighestVersionConfigurations(new MessageType(RESUME_EVENT))).thenReturn(List.of());
        registerRestorableWorkflow();
    }

    @Test
    void aResumeEventDeliveredBeforeARestoredBodyReRegistersItsWaitConditionStillWakesTheInstance() {
        var owner = owningSegment(RESIDENT_ID);
        workflowEngine.restoreWorkflowsFor(owner, null, sourcingContext(), new StubProcessingContext()).join();

        assertThat(restored).as("the claim materialized the instance").isNotNull();
        assertThat(restored.isRunning())
                .as("the claim marks the instance running the moment it submits the body").isTrue();

        // The segment's backlog lands here: running, but the body has not reached awaitEvent yet.
        workflowEngine.handle(resumeEvent(), deliveryContext(owner));

        // Only now does the body replay back to its awaitEvent and re-register.
        var waitStep = registerWaitFor(WAIT_STEP);

        drainOneTask();

        assertThat(waitStep.isCancelled())
                .as("""
                            The resume event was delivered to segment %s while '%s' was running but had not yet re-registered \
                            its wait condition. Waking the instance cancels the wait step's timeout future, so a cancelled \
                            future is the wake. Not cancelled means the event was matched against an empty condition set on \
                            the delivering thread and dropped: the instance then sits on its timeout with no warning and no \
                            error.""", owner, RESIDENT_ID)
                .isTrue();
    }

    @Test
    void aResumeEventDeliveredAfterTheWaitConditionIsRegisteredStillWakesTheInstance() {
        var owner = owningSegment(RESIDENT_ID);
        workflowEngine.restoreWorkflowsFor(owner, null, sourcingContext(), new StubProcessingContext()).join();

        var waitStep = registerWaitFor(WAIT_STEP);

        workflowEngine.handle(resumeEvent(), deliveryContext(owner));

        drainOneTask();

        assertThat(waitStep.isCancelled())
                .as("the ordinary live ordering - condition first, event second - must keep waking the instance")
                .isTrue();
    }

    /**
     * Stands in for the restored body arriving back at its {@code awaitEvent} call.
     */
    private CompletableFuture<Void> registerWaitFor(String stepName) {
        var timeoutFuture = new CompletableFuture<Void>();
        restored.registerRunningStep(stepName, timeoutFuture);
        restored.registerWaitCondition(stepName,
                                       EventConditions.fromQualifiedName(RESUME_EVENT),
                                       GlobalOnlyPayloadReducer.INSTANCE,
                                       defaults());
        return timeoutFuture;
    }

    /**
     * Runs the single task the delivery queued on the instance, the way the parked body would.
     */
    private void drainOneTask() {
        Consumer<WorkflowExecution> task = restored.getNextTask();
        assertThat(task).as("delivering the event queues exactly one task on the instance").isNotNull();
        task.accept(restored);
    }

    /**
     * Wires the store and the registry so {@link #RESIDENT_ID} can be rehydrated into a real
     * {@link SimpleWorkflowExecution} whose body is submitted to an executor that never runs it - the state a claim
     * leaves behind the moment it returns.
     */
    @SuppressWarnings("unchecked")
    private void registerRestorableWorkflow() {
        var running = new EventSourcedRunningWorkflows();
        running.evolve(MetadataUtils.create(RESIDENT_ID, WorkflowStatus.STARTED));
        when(workflowStore.loadRunningWorkflows(any())).thenReturn(CompletableFuture.completedFuture(running));
        when(workflowStore.loadWorkflow(eq(RESIDENT_ID), any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new EventSourcedWorkflowState(RESIDENT_ID, Map.of("orderId", RESIDENT_ID), DEFINITION_ID)));

        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        when(configuration.workflowName()).thenReturn("RestoredWorkflow");
        when(configuration.workflowVersion()).thenReturn("1.0.0");
        when(configuration.eventNameCustomizer()).thenReturn(defaults());
        when(configuration.workflowStatusChangeListeners()).thenReturn(Map.of());

        var workflowContext = mock(WorkflowContext.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory executionFactory = mock(WorkflowExecutionFactory.class);
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(contextFactory.createContext(anyMap(), eq(RESIDENT_ID), any(), eq(configuration)))
                .thenReturn(workflowContext);
        when(executionFactory.create(workflowContext)).thenAnswer(invocation -> {
            restored = new SimpleWorkflowExecution(RESIDENT_ID,
                                                   Map.of("orderId", RESIDENT_ID),
                                                   bodyContext(),
                                                   configuration,
                                                   workflowContext);
            return restored;
        });
        when(configurationRegistry.getWorkflowConfiguration(DEFINITION_ID)).thenReturn(Optional.of(configuration));
    }

    /**
     * The context a restored body runs under. Its executor never runs the submitted body, which freezes the instance in
     * the window this test is about.
     */
    private static ProcessingContext bodyContext() {
        var context = mock(ProcessingContext.class);
        when(context.resources()).thenReturn(Map.of());
        when(context.component(UnitOfWorkFactory.class)).thenReturn(mock(UnitOfWorkFactory.class));
        when(context.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(context.component(ExecutorService.class, WORKFLOW_ENGINE_EXECUTOR))
                .thenReturn(mock(ExecutorService.class));
        when(context.component(EventSink.class)).thenReturn(mock(EventSink.class));
        when(context.component(WorkflowScheduler.class)).thenReturn(mock(WorkflowScheduler.class));
        when(context.component(ExecuteStepActionResolver.class)).thenReturn(mock(ExecuteStepActionResolver.class));
        when(context.whenComplete(any())).thenAnswer(invocation -> {
            invocation.<Consumer<ProcessingContext>>getArgument(0).accept(context);
            return context;
        });
        return context;
    }

    /**
     * A processor batch context carrying the segment the event is delivered under.
     */
    private ProcessingContext deliveryContext(Segment segment) {
        var context = new StubProcessingContext();
        context.putResource(Segment.RESOURCE_KEY, segment);
        return context;
    }

    private ProcessingContext sourcingContext() {
        return new StubProcessingContext();
    }

    private static EventMessage resumeEvent() {
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(RESUME_EVENT));
        when(eventMessage.payloadAs(any(TypeReference.class))).thenReturn(Map.of("orderId", RESIDENT_ID));
        return eventMessage;
    }
}
