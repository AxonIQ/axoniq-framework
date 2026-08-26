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

import io.axoniq.framework.messaging.eventstreaming.checkpoint.CheckpointTrigger;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry.PredicatedWorkflowConfiguration;
import io.axoniq.workflow.runtime.execution.payload.PayloadReducerRegistry;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import jakarta.annotation.Nullable;
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.replay.ReplayStatus;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.lang.reflect.Field;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test for {@link WorkflowEngine} replay behavior.
 */
class WorkflowEngineReplayTest {

    private WorkflowEngine workflowEngine;
    private WorkflowExecutionRepository workflowExecutionRepository;
    private WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private WorkflowStore workflowStore;
    private UnitOfWorkFactory startupUnitOfWorkFactory;
    private WorkflowEngineReplaySupport replaySupport;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;

    @BeforeEach
    void setUp() {
        workflowExecutionRepository = spy(new InMemoryWorkflowExecutionRepository());
        workflowConfigurationRegistry = mock(WorkflowConfigurationRegistry.class);
        workflowStore = mock(WorkflowStore.class);
        startupUnitOfWorkFactory = mock(UnitOfWorkFactory.class);

        workflowEngine = new WorkflowEngine(
                workflowConfigurationRegistry,
                workflowExecutionRepository,
                new WorkflowCancellationService(),
                workflowStore,
                startupUnitOfWorkFactory
        );
        replaySupport = spy(new WorkflowEngineReplaySupport(workflowEngine));
        checkpointingSupport = new WorkflowEngineCheckpointingSupport(workflowEngine);
        workflowEngine.setEngineSupportComponents(replaySupport, checkpointingSupport);
    }

    @Test
    void replayEventsAreDeliveredToExecution() {
        String workflowId = "workflowId";
        WorkflowExecution execution = mock(WorkflowExecution.class);
        WorkflowState state = mock(WorkflowState.class);
        when(execution.state()).thenReturn(state);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        workflowExecutionRepository.save(workflowId, () -> execution);

        EventMessage eventMessage = mock(EventMessage.class);
        Metadata metaData = Metadata.with("workflowId", workflowId);
        when(eventMessage.metadata()).thenReturn(metaData);

        ProcessingContext processingContext = processingContext(null);
        when(processingContext.resources()).thenReturn(Map.of());
        when(processingContext.component(WorkflowEngineReplaySupport.class)).thenReturn(replaySupport);
        when(processingContext.component(WorkflowEngineCheckpointingSupport.class)).thenReturn(checkpointingSupport);

        // Keep replay in progress so the engine does not switch to live mode and try to (re)start executions.
        replaySupport.setInitialEngineTokens(token(0), token(1));

        workflowEngine.handle(eventMessage, processingContext);

        verify(execution).onEvent(eventMessage, processingContext);
    }

    @Test
    void replayFinishedCleanupAndExecute() {

        WorkflowExecution terminalExecution = mock(WorkflowExecution.class);
        WorkflowState terminalState = mock(WorkflowState.class);
        when(terminalExecution.workflowId()).thenReturn("terminalId");
        when(terminalExecution.state()).thenReturn(terminalState);
        when(terminalState.workflowStatus()).thenReturn(WorkflowStatus.COMPLETED);
        WorkflowContext terminalContext = mock(WorkflowContext.class);
        when(terminalExecution.workflowContext()).thenReturn(terminalContext);
        ProcessingContext terminalPC = mock(ProcessingContext.class);
        when(terminalContext.processingContext()).thenReturn(terminalPC);
        when(terminalPC.whenComplete(any())).thenReturn(terminalPC);

        WorkflowExecution runningExecution = cancellationCapableExecution();
        WorkflowState runningState = mock(WorkflowState.class);
        WorkflowContext runningContext = mock(WorkflowContext.class);
        when(runningExecution.workflowId()).thenReturn("runningId");
        when(runningExecution.state()).thenReturn(runningState);
        when(runningExecution.workflowContext()).thenReturn(runningContext);
        when(runningState.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        ProcessingContext runningPC = mock(ProcessingContext.class);
        when(runningContext.processingContext()).thenReturn(runningPC);
        when(runningPC.whenComplete(any())).thenAnswer(invocation -> {
            Consumer<ProcessingContext> consumer = invocation.getArgument(0);
            consumer.accept(runningPC);
            return runningPC;
        });

        workflowExecutionRepository.save("terminalId", () -> terminalExecution);
        workflowExecutionRepository.save("runningId", () -> runningExecution);

        ReplayStatusChanged replayStatusChanged = mock(ReplayStatusChanged.class);
        ReplayStatus status = mock(ReplayStatus.class);
        when(replayStatusChanged.status()).thenReturn(status);
        when(status.isReplay()).thenReturn(false);

        ProcessingContext context = processingContext(token(191));

        context.component(WorkflowEngineReplaySupport.class).handle(replayStatusChanged, context);

        verify(workflowExecutionRepository).removeAll(any());
        assertThat(workflowExecutionRepository.findById("terminalId")).isEmpty();

        verify(runningExecution, times(1)).execute(any());
        verify(terminalExecution, never()).execute(any()); //
    }

    @Test
    void replayStatusChangesIsExecutedFlag() {
        ProcessingContext pc = mock(ProcessingContext.class);
        WorkflowConfiguration<?> config = mock(WorkflowConfiguration.class);
        when(config.workflowName()).thenReturn("test-workflow");
        when(config.workflowVersion()).thenReturn(org.axonframework.messaging.core.MessageType.DEFAULT_VERSION);
        when(config.eventNameCustomizer()).thenReturn(defaults());

        when(pc.resources()).thenReturn(Map.of(TrackingToken.RESOURCE_KEY, token(18)));
        when(pc.component(UnitOfWorkFactory.class)).thenReturn(mock(UnitOfWorkFactory.class));
        when(pc.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(pc.component(eq(ExecutorService.class), any())).thenReturn(mock(ExecutorService.class));
        when(pc.component(EventSink.class)).thenReturn(mock(EventSink.class));
        when(pc.component(WorkflowScheduler.class)).thenReturn(mock(WorkflowScheduler.class));
        when(pc.component(ExecuteStepActionResolver.class)).thenReturn(mock(ExecuteStepActionResolver.class));

        WorkflowContext workflowContext = mock(WorkflowContext.class);
        when(workflowContext.processingContext()).thenReturn(pc);

        SimpleWorkflowExecution execution = new SimpleWorkflowExecution(
                "id",
                Map.of(),
                pc,
                config,
                workflowContext
        );

        // Initially not running (replay mode)
        assertThat(execution.isRunning()).isFalse();

        EventMessage event = mock(EventMessage.class);
        when(event.metadata()).thenReturn(Metadata.with("none", "none"));

        execution.onEvent(event, pc);
        assertThat(execution.isRunning()).isFalse();

        // Trigger start after replay
        execution.execute(i -> {
        });
        assertThat(execution.isRunning()).isTrue();
    }

    @Test
    void replayDoesNotInvokeWorkflowStatusListenersButLiveEventsDo() throws Exception {
        var replayListener = mock(WorkflowStatusChangeListener.class);
        var liveListener = mock(WorkflowStatusChangeListener.class);
        var execution = simpleExecution(
                "listener-workflow",
                token(18),
                Map.of(WorkflowStatus.STARTED, replayListener, WorkflowStatus.COMPLETED, liveListener)
        );
        var processingContext = execution.processingContext();
        var definitionId = new MessageType(new QualifiedName("CheckpointWorkflow"), MessageType.DEFAULT_VERSION);

        execution.onEvent(workflowStartedEvent("listener-workflow", definitionId, Map.of()), processingContext);

        assertThat(execution.state().workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
        verifyNoInteractions(replayListener, liveListener);

        markRunning(execution, true);
        execution.onEvent(workflowStatusEvent("listener-workflow", WorkflowStatus.COMPLETED), processingContext);
        drainAllTasks(execution);

        verify(liveListener).onWorkflowStatus(eq(WorkflowStatus.COMPLETED), any());
        verifyNoInteractions(replayListener);
    }

    @Test
    @SuppressWarnings("unchecked")
    void segmentClaimRestoresExecutionStateUsingSeparateSourcingAndExecutionContexts() {
        String workflowId = "wf-1";
        TrackingToken checkpointToken = token(30);
        ProcessingContext processingContext = processingContext(checkpointToken);
        when(processingContext.component(PayloadReducerRegistry.class)).thenReturn(new PayloadReducerRegistry());

        var running = new EventSourcedRunningWorkflows();
        running.evolve(MetadataUtils.create(workflowId, WorkflowStatus.STARTED));
        when(workflowStore.loadRunningWorkflows(same(processingContext)))
                .thenReturn(CompletableFuture.completedFuture(running));

        var definitionId = new MessageType(new QualifiedName("RestoredWorkflow"), "1.0.0");
        var restoredState = new EventSourcedWorkflowState(workflowId, definitionId);
        restoredState.evolve(workflowStartedEvent(workflowId, definitionId, Map.of("mode", "wait")), processingContext);
        restoredState.evolve(stepStartedEvent(workflowId, "waitForResume"), processingContext);
        when(workflowStore.loadWorkflow(eq(workflowId), same(processingContext)))
                .thenReturn(CompletableFuture.completedFuture(restoredState));

        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory executionFactory = mock(WorkflowExecutionFactory.class);
        WorkflowContext workflowContext = mock(WorkflowContext.class);

        when(configuration.workflowName()).thenReturn("RestoredWorkflow");
        when(configuration.workflowVersion()).thenReturn(definitionId.version());
        when(configuration.eventNameCustomizer()).thenReturn(defaults());
        when(configuration.workflowStatusChangeListeners()).thenReturn(Map.of());
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(workflowConfigurationRegistry.getWorkflowConfiguration(definitionId)).thenReturn(Optional.of(configuration));
        when(contextFactory.createContext(anyMap(), eq(workflowId), any(ProcessingContext.class), eq(configuration)))
                .thenReturn(workflowContext);
        var restoredExecution = mock(WorkflowExecution.class);
        when(restoredExecution.workflowId()).thenReturn(workflowId);
        when(restoredExecution.state()).thenReturn(restoredState);
        when(restoredExecution.isRunning()).thenReturn(true);
        when(executionFactory.create(workflowContext)).thenReturn(restoredExecution);

        var startupUnitOfWork = mock(UnitOfWork.class);
        when(startupUnitOfWorkFactory.create("WorkflowRehydration")).thenReturn(startupUnitOfWork);
        when(startupUnitOfWork.executeWithResult(any()))
                .thenAnswer(invocation -> invocation.<java.util.function.Function<ProcessingContext, CompletableFuture<Void>>>getArgument(
                        0).apply(processingContext));

        workflowEngine.start(checkpointToken, false).join();
        ProcessingContext executionContext = processingContext(checkpointToken);
        // A single segment owns every instance, keeping this test about the two contexts rather than about ownership.
        workflowEngine.restoreWorkflowsFor(new Segment(0, 0), checkpointToken, processingContext, executionContext)
                      .join();

        verify(restoredExecution).initializeState(restoredState);
        verify(replaySupport).setCurrentTokenIfNull(checkpointToken);
        verify(replaySupport).switchToLiveMode(processingContext);
        var capturedExecutionContext = ArgumentCaptor.forClass(ProcessingContext.class);
        verify(contextFactory)
                .createContext(anyMap(), eq(workflowId), capturedExecutionContext.capture(), eq(configuration));
        assertThat(capturedExecutionContext.getValue()).isNotSameAs(processingContext);
    }

    @Test
    void crossVersionStartDisambiguatesWorkflowIdWithVersionSuffix() {
        String baseId = "order-1";

        // Pre-register a running v1.0.0 workflow under the base id.
        WorkflowExecution existing = mock(WorkflowExecution.class);
        WorkflowState existingState = mock(WorkflowState.class);
        when(existing.state()).thenReturn(existingState);
        when(existingState.workflowDefinitionId()).thenReturn(new MessageType("TestWorkflow", "1.0.0"));
        when(existingState.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        workflowExecutionRepository.save(baseId, () -> existing);
        clearInvocations(workflowExecutionRepository);

        // A v2.0.0 configuration that would resolve to the same base id.
        WorkflowConfiguration<?> v2 = mock(WorkflowConfiguration.class);
        WorkflowIdProvider idProvider = event -> baseId;
        when(v2.workflowIdProvider()).thenReturn(idProvider);
        when(v2.workflowVersion()).thenReturn("2.0.0");
        // Other lookups are exercised after disambiguation - return safe stubs.
        var contextFactory = mock(io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory.class);
        var workflowContext = mock(WorkflowContext.class);
        when(contextFactory.createContext(anyMap(), anyString(), any(), any())).thenReturn(workflowContext);
        when(v2.workflowContextFactory()).thenReturn(contextFactory);
        var executionFactory = mock(io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory.class);
        var execution = cancellationCapableExecution();
        when(execution.workflowId()).thenReturn(baseId + "#2.0.0");
        when(executionFactory.create(any())).thenReturn(execution);
        when(v2.workflowExecutionFactory()).thenReturn(executionFactory);

        QualifiedName eventName = new QualifiedName("OrderPlaced");
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(new MessageType(eventName)))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, v2)));

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));
        when(eventMessage.payloadAs(any(org.axonframework.common.TypeReference.class))).thenReturn(Map.of());

        ProcessingContext processingContext = processingContext(null);

        // Keep replay in progress so the engine does not switch to live mode and try to (re)start executions.
        replaySupport.setInitialEngineTokens(token(0), token(1));

        workflowEngine.handle(eventMessage, processingContext);

        // A v2 start was saved under the disambiguated id.
        verify(workflowExecutionRepository).save(eq(baseId + "#2.0.0"), any());
        // The base id was NOT reused - the v1 instance is untouched.
        verify(workflowExecutionRepository, never()).save(eq(baseId), any());
    }

    @Test
    void sameVersionDuplicateStartIsRejected() {
        String workflowId = "order-1";

        // Pre-register a running v2.0.0 workflow under "order-1".
        WorkflowExecution existing = mock(WorkflowExecution.class);
        WorkflowState existingState = mock(WorkflowState.class);
        when(existing.state()).thenReturn(existingState);
        when(existingState.workflowDefinitionId()).thenReturn(new MessageType("TestWorkflow", "2.0.0"));
        when(existingState.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        workflowExecutionRepository.save(workflowId, () -> existing);
        clearInvocations(workflowExecutionRepository);

        // A second v2.0.0 configuration arriving for the same base id.
        WorkflowConfiguration<?> v2 = mock(WorkflowConfiguration.class);
        when(v2.workflowIdProvider()).thenReturn(event -> workflowId);
        when(v2.workflowVersion()).thenReturn("2.0.0");

        QualifiedName eventName = new QualifiedName("OrderPlaced");
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(new MessageType(eventName)))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, v2)));

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));

        // Keep replay in progress so the engine does not switch to live mode and try to (re)start executions.
        replaySupport.setInitialEngineTokens(token(0), token(1));

        workflowEngine.handle(eventMessage, processingContext(null));

        // Same-version duplicate: nothing is started. The v1 (and this case v2) instance stays untouched.
        verify(workflowExecutionRepository, never()).save(anyString(), any());
    }

    @Test
    void duplicateWorkflowIdStartRequestIsIgnored() {
        String workflowId = "dup-id";

        // Pre-register a running workflow under "dup-id" at version "1.0.0".
        WorkflowExecution existing = mock(WorkflowExecution.class);
        WorkflowState existingState = mock(WorkflowState.class);
        when(existing.state()).thenReturn(existingState);
        when(existingState.workflowDefinitionId()).thenReturn(new MessageType("TestWorkflow", "1.0.0"));
        when(existingState.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        workflowExecutionRepository.save(workflowId, () -> existing);
        clearInvocations(workflowExecutionRepository);

        // A configuration that matches the incoming event and resolves to the same id AT THE SAME VERSION.
        // This is the "same-version duplicate" branch of resolveWorkflowIdForNewInstance - should be rejected.
        WorkflowConfiguration<?> configuration = mock(WorkflowConfiguration.class);
        WorkflowIdProvider idProvider = event -> workflowId;
        when(configuration.workflowIdProvider()).thenReturn(idProvider);
        when(configuration.workflowVersion()).thenReturn("1.0.0");

        QualifiedName eventName = new QualifiedName("OrderPlaced");
        // Stub the version-aware helper that checkAndCreateNewInstance actually invokes.
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(new MessageType(eventName)))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, configuration)));

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));

        ProcessingContext processingContext = processingContext(null);

        // Keep replay in progress so the engine does not switch to live mode and try to (re)start executions.
        replaySupport.setInitialEngineTokens(token(0), token(1));

        workflowEngine.handle(eventMessage, processingContext);

        // No new workflow is created for the same-version duplicate. The dedup path actually executed
        // because the version-aware lookup was stubbed correctly.
        verify(workflowExecutionRepository, never()).save(anyString(), any());
        verify(configuration, never()).workflowExecutionFactory();
        verify(configuration, never()).workflowContextFactory();

        // The pre-existing workflow still receives the event through the routing loop
        verify(existing).onEvent(eventMessage, processingContext);
    }

    @Test
    void replayEventsUseUnwrappedTokensForCheckpointRequests() {
        QualifiedName eventName = new QualifiedName("OrderPlaced");
        TrackingToken safePoint = token(10);
        TrackingToken tokenAtReset = token(20);
        TrackingToken firstReplayToken = ReplayToken.createReplayToken(tokenAtReset, token(10));
        TrackingToken secondReplayToken = ReplayToken.createReplayToken(tokenAtReset, token(15));

        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory executionFactory = mock(WorkflowExecutionFactory.class);
        CheckpointTrigger trigger = mock(CheckpointTrigger.class);
        Map<WorkflowContext, String> workflowIdsByContext = new HashMap<>();

        when(configuration.workflowIdProvider())
                .thenReturn(event -> String.valueOf(event.payloadAs(new TypeReference<Map<String, Object>>() {
                }).get("orderId")));
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(new MessageType(eventName)))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, configuration)));
        when(contextFactory.createContext(anyMap(), anyString(), any(), eq(configuration))).thenAnswer(invocation -> {
            String workflowId = invocation.getArgument(1);
            var workflowContext = mock(WorkflowContext.class);
            ProcessingContext workflowProcessingContext = mock(ProcessingContext.class);
            when(workflowContext.processingContext()).thenReturn(workflowProcessingContext);
            when(workflowProcessingContext.whenComplete(any())).thenReturn(workflowProcessingContext);
            workflowIdsByContext.put(workflowContext, workflowId);
            return workflowContext;
        });
        when(executionFactory.create(any())).thenAnswer(invocation -> {
            WorkflowContext workflowContext = invocation.getArgument(0);
            var execution = cancellationCapableExecution();
            var state = mock(WorkflowState.class);
            when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
            when(execution.workflowId()).thenReturn(workflowIdsByContext.get(workflowContext));
            when(execution.workflowContext()).thenReturn(workflowContext);
            when(execution.state()).thenReturn(state);
            return execution;
        });

        replaySupport.setInitialEngineTokens(safePoint, tokenAtReset);
        checkpointingSupport.onSegmentClaimed(Segment.ROOT_SEGMENT, null, trigger);

        workflowEngine.handle(startEvent(eventName, "wf-1"), segmentedContext(firstReplayToken));
        workflowEngine.handle(startEvent(eventName, "wf-2"), segmentedContext(secondReplayToken));

        verify(trigger).requestCheckpoint(token(10));
        verify(trigger).requestCheckpoint(token(15));
    }

    @Test
    void checkpointAdvancesToRequestedTokenWhenNoWorkflowWorkIsPending() {
        var requested = token(25);

        replaySupport.setInitialEngineTokens(token(18), token(30));

        var advanced = checkpointingSupport.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested).join();

        assertSameToken(advanced, requested);
    }

    @Test
    void checkpointWaitsForWorkflowQueueToDrain() throws Exception {
        var execution = simpleExecution("wf-1", token(18));
        registerExecutionWithEngine(execution);
        markRunning(execution, true);
        execution.appendTask(ignored -> {
        });

        var trigger = mock(CheckpointTrigger.class);
        replaySupport.setInitialEngineTokens(token(18), token(30));
        checkpointingSupport.onSegmentClaimed(Segment.ROOT_SEGMENT, null, trigger);

        var requested = token(25);
        var advanced = checkpointingSupport.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

        assertThat(advanced).isNotDone();

        drainAllTasks(execution);

        assertSameToken(advanced.join(), requested);
    }

    @Test
    void checkpointIsRequestedImmediatelyEvenWhenWorkflowQueueHasPendingWork() throws Exception {
        var execution = simpleExecution("wf-1", token(18));
        registerExecutionWithEngine(execution);
        markRunning(execution, true);
        execution.appendTask(ignored -> {
        });

        var trigger = mock(CheckpointTrigger.class);
        var requested = token(25);
        replaySupport.setInitialEngineTokens(token(18), token(30));
        checkpointingSupport.onSegmentClaimed(Segment.ROOT_SEGMENT, null, trigger);

        checkpointingSupport.requestCheckpoint(Segment.ROOT_SEGMENT, requested);

        verify(trigger).requestCheckpoint(requested);
    }

    /**
     * A request made while the segment is not claimed has no trigger to reach and is dropped rather than held: the
     * claim it was safe for is not this one, and replaying it into the next claim would advance that claim's stored
     * token past events it has to redeliver.
     */
    @Test
    void checkpointRequestsMadeWhileTheSegmentIsNotClaimedAreIgnored() throws Exception {
        var trigger = mock(CheckpointTrigger.class);
        var beforeClaim = token(25);
        var afterClaim = token(27);
        replaySupport.setInitialEngineTokens(token(18), token(30));

        checkpointingSupport.requestCheckpoint(Segment.ROOT_SEGMENT, beforeClaim);

        verifyNoInteractions(trigger);

        checkpointingSupport.onSegmentClaimed(Segment.ROOT_SEGMENT, null, trigger);
        checkpointingSupport.requestCheckpoint(Segment.ROOT_SEGMENT, afterClaim);

        verify(trigger).requestCheckpoint(afterClaim);
        verifyNoMoreInteractions(trigger);
    }

    @Test
    void checkpointRequestsAreForwardedWhileEarlierAdvanceIsStillInFlight() throws Exception {
        var trigger = mock(CheckpointTrigger.class);
        var firstRequested = token(25);
        var secondRequested = token(27);
        replaySupport.setInitialEngineTokens(token(18), token(30));
        checkpointingSupport.onSegmentClaimed(Segment.ROOT_SEGMENT, null, trigger);

        checkpointingSupport.requestCheckpoint(Segment.ROOT_SEGMENT, firstRequested);
        checkpointingSupport.requestCheckpoint(Segment.ROOT_SEGMENT, secondRequested);

        verify(trigger).requestCheckpoint(firstRequested);
        verify(trigger).requestCheckpoint(secondRequested);
    }

    @Test
    void checkpointAdvanceRechecksWhenEarlierTaskAppendsMoreWorkBehindBarrier() throws Exception {
        var execution = simpleExecution("wf-1", token(18));
        registerExecutionWithEngine(execution);
        markRunning(execution, true);
        execution.appendTask(ignored -> execution.appendTask(next -> {
        }));

        var requested = token(25);
        var advanced = checkpointingSupport.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

        execution.getNextTask().accept(execution);
        assertThat(advanced).isNotDone();

        execution.getNextTask().accept(execution);
        assertThat(advanced).isNotDone();

        execution.getNextTask().accept(execution);
        assertThat(advanced).isNotDone();

        execution.getNextTask().accept(execution);
        assertSameToken(advanced.join(), requested);
    }

    @Test
    void checkpointIntentCallbacksAreCoalescedIntoSingleQueuedTask() throws Exception {
        var execution = simpleExecution("wf-1", token(18));
        markRunning(execution, true);
        AtomicInteger drainedCallbacks = new AtomicInteger();

        execution.addCheckpointLatch(drainedCallbacks::incrementAndGet);
        execution.addCheckpointLatch(drainedCallbacks::incrementAndGet);

        Consumer<WorkflowExecution> queuedTask = execution.getNextTask();

        assertThat(queuedTask).isNotNull();
        assertThat(execution.getNextTask()).isNull();
        assertThat(drainedCallbacks).hasValue(0);

        queuedTask.accept(execution);

        assertThat(drainedCallbacks).hasValue(2);
    }

    private static TrackingToken token(long globalIndex) {
        return new GlobalSequenceTrackingToken(globalIndex);
    }

    /**
     * A processor batch context: it carries the segment being handled, exactly as a work package's context does.
     */
    private ProcessingContext segmentedContext(TrackingToken token) {
        ProcessingContext processingContext = processingContext(token);
        when(processingContext.getResource(Segment.RESOURCE_KEY)).thenReturn(Segment.ROOT_SEGMENT);
        return processingContext;
    }

    private ProcessingContext processingContext(TrackingToken token) {
        ProcessingContext processingContext = mock(ProcessingContext.class);
        Map<Context.ResourceKey<?>, Object> resources = new HashMap<>();
        if (token != null) {
            resources.put(TrackingToken.RESOURCE_KEY, token);
        }
        when(processingContext.resources()).thenReturn(resources);
        when(processingContext.getResource(any())).thenAnswer(invocation -> resources.get(invocation.getArgument(0)));
        doAnswer(invocation -> {
            Context.ResourceKey<?> key = invocation.getArgument(0);
            Object value = invocation.getArgument(1);
            resources.put(key, value);
            return processingContext;
        }).when(processingContext).putResource(any(), any());
        when(processingContext.component(WorkflowEngineReplaySupport.class)).thenReturn(replaySupport);
        when(processingContext.component(WorkflowEngineCheckpointingSupport.class)).thenReturn(checkpointingSupport);
        return processingContext;
    }

    private static EventMessage startEvent(QualifiedName eventName, String workflowId) {
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));
        when(eventMessage.payloadAs(any(TypeReference.class))).thenReturn(Map.of("orderId", workflowId));
        return eventMessage;
    }

    private SimpleWorkflowExecution simpleExecution(String workflowId,
                                                    TrackingToken restartToken) {
        return simpleExecution(workflowId, restartToken, Map.of());
    }

    private void registerExecutionWithEngine(SimpleWorkflowExecution execution) {
        QualifiedName eventName = new QualifiedName("RegisterCheckpointWork");
        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory executionFactory = mock(WorkflowExecutionFactory.class);
        when(configuration.workflowIdProvider()).thenReturn(event -> execution.workflowId());
        when(configuration.workflowVersion()).thenReturn(MessageType.DEFAULT_VERSION);
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(contextFactory.createContext(anyMap(), anyString(), any(), eq(configuration))).thenReturn(
                mock(WorkflowContext.class)
        );
        when(executionFactory.create(any())).thenReturn(execution);
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(new MessageType(eventName))).thenReturn(
                List.of(new PredicatedWorkflowConfiguration((event, context) -> true, configuration))
        );

        workflowEngine.handle(startEvent(eventName, execution.workflowId()), processingContext(null));
        // The same delivery also reaches the instance it just started, which queues the wait-condition match because
        // its body has not started. Draining it here leaves a quiescent instance, so each test below controls exactly
        // what sits in the queue.
        drainAllTasks(execution);
    }

    private SimpleWorkflowExecution simpleExecution(
            String workflowId,
            TrackingToken restartToken,
            Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners
    ) {
        ProcessingContext processingContext = processingContext(restartToken);
        when(processingContext.component(UnitOfWorkFactory.class)).thenReturn(mock(UnitOfWorkFactory.class));
        when(processingContext.component(PayloadReducerRegistry.class)).thenReturn(new PayloadReducerRegistry());
        when(processingContext.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(processingContext.component(eq(ExecutorService.class), any())).thenReturn(mock(ExecutorService.class));
        when(processingContext.component(EventSink.class)).thenReturn(mock(EventSink.class));
        when(processingContext.component(WorkflowScheduler.class)).thenReturn(mock(WorkflowScheduler.class));
        when(processingContext.component(ExecuteStepActionResolver.class)).thenReturn(
                mock(ExecuteStepActionResolver.class)
        );

        WorkflowConfiguration<WorkflowContext> config = mock(WorkflowConfiguration.class);
        when(config.workflowName()).thenReturn("CheckpointWorkflow");
        when(config.workflowVersion()).thenReturn(MessageType.DEFAULT_VERSION);
        when(config.eventNameCustomizer()).thenReturn(defaults());
        when(config.workflowStatusChangeListeners()).thenReturn(workflowStatusChangeListeners);

        WorkflowContext workflowContext = mock(WorkflowContext.class);
        when(workflowContext.processingContext()).thenReturn(processingContext);

        return new SimpleWorkflowExecution(
                workflowId,
                Map.of("id", workflowId),
                processingContext,
                config,
                workflowContext
        );
    }

    private static void markRunning(SimpleWorkflowExecution execution, boolean running) throws Exception {
        Field field = SimpleWorkflowExecution.class.getDeclaredField("running");
        field.setAccessible(true);
        field.set(execution, running);
    }

    private static void drainAllTasks(SimpleWorkflowExecution execution) {
        Consumer<WorkflowExecution> task;
        while ((task = execution.getNextTask()) != null) {
            task.accept(execution);
        }
    }

    private static EventMessage workflowStartedEvent(String workflowId,
                                                     MessageType definitionId,
                                                     Map<String, Object> payload) {
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(MetadataUtils.create(workflowId, WorkflowStatus.STARTED, definitionId)
                                                              .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD,
                                                                   io.axoniq.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer.NAME));
        when(eventMessage.type()).thenReturn(new MessageType("RestoredWorkflowStarted", definitionId.version()));
        when(eventMessage.payloadAs(Object.class)).thenReturn(payload);
        when(eventMessage.payloadAs(any(TypeReference.class))).thenReturn(payload);
        when(eventMessage.timestamp()).thenReturn(java.time.Instant.now());
        return eventMessage;
    }

    private static EventMessage workflowStatusEvent(String workflowId, WorkflowStatus status) {
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(MetadataUtils.create(workflowId, status));
        when(eventMessage.type()).thenReturn(new MessageType("Workflow" + status));
        when(eventMessage.payloadAs(Object.class)).thenReturn(Map.of());
        when(eventMessage.timestamp()).thenReturn(java.time.Instant.now());
        return eventMessage;
    }

    private static EventMessage stepStartedEvent(String workflowId, String stepName) {
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(MetadataUtils.create(
                workflowId,
                stepName,
                io.axoniq.workflow.runtime.api.execution.status.StepStatus.STARTED
        ));
        when(eventMessage.type()).thenReturn(new MessageType(stepName + "Started"));
        when(eventMessage.payloadAs(Object.class)).thenReturn(Map.of("stepName", stepName));
        when(eventMessage.timestamp()).thenReturn(java.time.Instant.now());
        return eventMessage;
    }

    @Test
    void crossVersionStartDisambiguatedIdAlsoTakenIsRejected() {
        // Both the base id and the disambiguated id are already occupied.
        String baseId = "order-1";
        String disambiguatedId = baseId + "#2.0.0";

        WorkflowExecution v1Existing = mock(WorkflowExecution.class);
        WorkflowState v1State = mock(WorkflowState.class);
        when(v1Existing.state()).thenReturn(v1State);
        when(v1State.workflowDefinitionId()).thenReturn(new MessageType("TestWorkflow", "1.0.0"));
        when(v1State.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        workflowExecutionRepository.save(baseId, () -> v1Existing);

        WorkflowExecution v2Existing = mock(WorkflowExecution.class);
        WorkflowState v2State = mock(WorkflowState.class);
        when(v2Existing.state()).thenReturn(v2State);
        when(v2State.workflowDefinitionId()).thenReturn(new MessageType("TestWorkflow", "2.0.0"));
        when(v2State.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        workflowExecutionRepository.save(disambiguatedId, () -> v2Existing);
        clearInvocations(workflowExecutionRepository);

        WorkflowConfiguration<?> v2 = mock(WorkflowConfiguration.class);
        when(v2.workflowIdProvider()).thenReturn(event -> baseId);
        when(v2.workflowVersion()).thenReturn("2.0.0");

        QualifiedName eventName = new QualifiedName("OrderPlaced");
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(new MessageType(eventName)))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, v2)));

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));

        // Keep replay in progress so the engine does not switch to live mode and try to (re)start executions.
        replaySupport.setInitialEngineTokens(token(0), token(1));

        workflowEngine.handle(eventMessage, processingContext(null));

        // Even the disambiguated id is taken - nothing new is started.
        verify(workflowExecutionRepository, never()).save(anyString(), any());
    }

    private WorkflowExecution cancellationCapableExecution() {
        var execution = mock(SimpleWorkflowExecution.class);
        var cancellation = mock(WorkflowCancellation.class);
        when(execution.workflowCancellation()).thenReturn(cancellation);
        return execution;
    }

    private static void assertSameToken(@Nullable TrackingToken actual, @Nullable TrackingToken expected) {
        assertThat(actual).isNotNull();
        assertThat(expected).isNotNull();
        assertThat(actual.lowerBound(expected)).isEqualTo(expected);
        assertThat(expected.lowerBound(actual)).isEqualTo(actual);
    }
}
