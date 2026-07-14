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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowDefinitionId;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowIdProvider;
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
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.replay.ReplayStatus;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static io.axoniq.workflow.runtime.util.ProcessingContextUtils.RESTART_TOKEN_RESOURCE_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test for {@link WorkflowEngine} replay behavior.
 */
class WorkflowEngineReplayTest {

    private WorkflowEngine workflowEngine;
    private WorkflowExecutionRepository workflowExecutionRepository;
    private WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private SafePointStore safePointStore;
    private WorkflowStateRehydrationSupport workflowStateRehydrationSupport;

    @BeforeEach
    void setUp() {
        workflowExecutionRepository = spy(new InMemoryWorkflowExecutionRepository());
        workflowConfigurationRegistry = mock(WorkflowConfigurationRegistry.class);
        safePointStore = mock(SafePointStore.class);
        workflowStateRehydrationSupport = mock(WorkflowStateRehydrationSupport.class);
        workflowEngine = new WorkflowEngine(
                workflowConfigurationRegistry,
                workflowExecutionRepository,
                safePointStore,
                workflowStateRehydrationSupport
        );
    }

    @Test
    void replayEventsAreDeliveredToExecution() {
        String workflowId = "workflowId";
        WorkflowExecution execution = mock(WorkflowExecution.class);
        workflowExecutionRepository.save(workflowId, () -> execution);

        EventMessage eventMessage = mock(EventMessage.class);
        Metadata metaData = Metadata.with("workflowId", workflowId);
        when(eventMessage.metadata()).thenReturn(metaData);

        ProcessingContext processingContext = mock(ProcessingContext.class);
        when(processingContext.resources()).thenReturn(Map.of());

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

        WorkflowExecution runningExecution = mock(WorkflowExecution.class);
        WorkflowState runningState = mock(WorkflowState.class);
        WorkflowContext runningContext = mock(WorkflowContext.class);
        when(runningExecution.workflowId()).thenReturn("runningId");
        when(runningExecution.state()).thenReturn(runningState);
        when(runningExecution.workflowContext()).thenReturn(runningContext);
        when(runningExecution.restartToken()).thenReturn(token(18));
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

        workflowEngine.handle(replayStatusChanged, context);

        // Verify terminal execution removed
        verify(workflowExecutionRepository).remove("terminalId");

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

        when(pc.resources()).thenReturn(Map.of(
                TrackingToken.RESOURCE_KEY, token(18),
                RESTART_TOKEN_RESOURCE_KEY, Optional.empty()
        ));
        when(pc.component(UnitOfWorkFactory.class)).thenReturn(mock(UnitOfWorkFactory.class));
        when(pc.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(pc.component(eq(ExecutorService.class), any())).thenReturn(mock(ExecutorService.class));
        when(pc.component(EventSink.class)).thenReturn(mock(EventSink.class));

        WorkflowContext workflowContext = mock(WorkflowContext.class);
        when(workflowContext.processingContext()).thenReturn(pc);

        SimpleWorkflowExecution execution = new SimpleWorkflowExecution(
                "id",
                Map.of(),
                pc,
                config,
                workflowContext
        );

        assertSameToken(execution.restartToken(), token(18));
        // Initially NOT executable (replay mode)
        assertThat(execution.isExecutable()).isFalse();

        EventMessage event = mock(EventMessage.class);
        when(event.metadata()).thenReturn(Metadata.with("none", "none"));

        execution.onEvent(event, pc);
        assertThat(execution.isExecutable()).isFalse();

        // Trigger switch to executable (end of replay)
        execution.execute(i -> {
        });
        assertThat(execution.isExecutable()).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void rehydrateRunningWorkflowsRestoresExecutionStateAndRestartToken() {
        String workflowId = "wf-1";
        TrackingToken checkpointToken = token(30);
        ProcessingContext restoreContext = processingContext(checkpointToken);
        ProcessingContext executionContext = processingContext(checkpointToken);
        when(restoreContext.component(PayloadReducerRegistry.class)).thenReturn(new PayloadReducerRegistry());
        when(restoreContext.component(UnitOfWorkFactory.class)).thenReturn(mock(UnitOfWorkFactory.class));
        when(restoreContext.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(restoreContext.component(eq(ExecutorService.class), any())).thenReturn(mock(ExecutorService.class));
        when(restoreContext.component(EventSink.class)).thenReturn(mock(EventSink.class));
        when(executionContext.component(PayloadReducerRegistry.class)).thenReturn(new PayloadReducerRegistry());
        when(executionContext.component(UnitOfWorkFactory.class)).thenReturn(mock(UnitOfWorkFactory.class));
        when(executionContext.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(executionContext.component(eq(ExecutorService.class), any())).thenReturn(mock(ExecutorService.class));
        when(executionContext.component(EventSink.class)).thenReturn(mock(EventSink.class));

        var running = new RunningWorkflows();
        running.evolve(MetadataUtils.create(workflowId, WorkflowStatus.STARTED));
        when(workflowStateRehydrationSupport.loadRunningWorkflows(same(restoreContext))).thenReturn(running);

        var definitionId = new WorkflowDefinitionId(new QualifiedName("RestoredWorkflow"), "1.0.0");
        var restoredState = new EventSourcedWorkflowState(workflowId, definitionId);
        restoredState.evolve(workflowStartedEvent(workflowId, definitionId, Map.of("mode", "wait")), restoreContext);
        restoredState.evolve(stepStartedEvent(workflowId, "waitForResume"), restoreContext);
        when(workflowStateRehydrationSupport.loadWorkflowState(eq(workflowId), same(restoreContext)))
                .thenReturn(restoredState);

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
        when(contextFactory.createContext(anyMap(), eq(workflowId), same(executionContext), eq(configuration)))
                .thenReturn(workflowContext);
        executionContext.putResource(RESTART_TOKEN_RESOURCE_KEY, Optional.of(checkpointToken));
        var restoredExecution = new SimpleWorkflowExecution(
                workflowId,
                restoredState.payload(),
                executionContext,
                configuration,
                workflowContext
        );
        when(executionFactory.create(workflowContext)).thenReturn(restoredExecution);

        workflowEngine.initializeSafePoint(checkpointToken);
        workflowEngine.rehydrateRunningWorkflows(restoreContext, executionContext);

        var restored = workflowEngine.workflowExecutions().iterator().next();
        assertThat(restored.workflowId()).isEqualTo(workflowId);
        assertThat(restored.state().payload()).containsEntry("mode", "wait");
        assertThat(restored.state().workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
        assertThat(restored.state().workflowDefinitionVersion()).isEqualTo(definitionId.version());
        assertThat(restored.state().getStep("waitForResume").status()).isEqualTo(
                io.axoniq.workflow.runtime.api.execution.status.StepStatus.STARTED
        );
        assertSameToken(restored.restartToken(), checkpointToken);
    }

    @Test
    void crossVersionStart_disambiguatesWorkflowIdWithVersionSuffix() {
        String baseId = "order-1";

        // Pre-register a running v1.0.0 workflow under the base id.
        WorkflowExecution existing = mock(WorkflowExecution.class);
        WorkflowState existingState = mock(WorkflowState.class);
        when(existing.state()).thenReturn(existingState);
        when(existingState.workflowDefinitionVersion()).thenReturn("1.0.0");
        workflowExecutionRepository.save(baseId, () -> existing);
        clearInvocations(workflowExecutionRepository);

        // A v2.0.0 configuration that would resolve to the same base id.
        WorkflowConfiguration<?> v2 = mock(WorkflowConfiguration.class);
        WorkflowIdProvider idProvider = event -> baseId;
        when(v2.workflowIdProvider()).thenReturn(idProvider);
        when(v2.workflowVersion()).thenReturn("2.0.0");
        // Other lookups are exercised after disambiguation — return safe stubs.
        var contextFactory = mock(io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory.class);
        var workflowContext = mock(WorkflowContext.class);
        when(contextFactory.createContext(anyMap(), anyString(), any(), any())).thenReturn(workflowContext);
        when(v2.workflowContextFactory()).thenReturn(contextFactory);
        var executionFactory = mock(io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory.class);
        when(executionFactory.create(any())).thenReturn(mock(WorkflowExecution.class));
        when(v2.workflowExecutionFactory()).thenReturn(executionFactory);

        QualifiedName eventName = new QualifiedName("OrderPlaced");
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(eventName))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, v2)));

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));
        when(eventMessage.payloadAs(any(org.axonframework.common.TypeReference.class))).thenReturn(Map.of());

        ProcessingContext processingContext = mock(ProcessingContext.class);

        workflowEngine.handle(eventMessage, processingContext);

        // A v2 spawn was saved under the disambiguated id.
        verify(workflowExecutionRepository).save(eq(baseId + "#2.0.0"), any());
        // The base id was NOT reused — the v1 instance is untouched.
        verify(workflowExecutionRepository, never()).save(eq(baseId), any());
    }

    @Test
    void sameVersionDuplicateStart_isRejected() {
        String workflowId = "order-1";

        // Pre-register a running v2.0.0 workflow under "order-1".
        WorkflowExecution existing = mock(WorkflowExecution.class);
        WorkflowState existingState = mock(WorkflowState.class);
        when(existing.state()).thenReturn(existingState);
        when(existingState.workflowDefinitionVersion()).thenReturn("2.0.0");
        workflowExecutionRepository.save(workflowId, () -> existing);
        clearInvocations(workflowExecutionRepository);

        // A second v2.0.0 configuration arriving for the same base id.
        WorkflowConfiguration<?> v2 = mock(WorkflowConfiguration.class);
        when(v2.workflowIdProvider()).thenReturn(event -> workflowId);
        when(v2.workflowVersion()).thenReturn("2.0.0");

        QualifiedName eventName = new QualifiedName("OrderPlaced");
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(eventName))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, v2)));

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));

        workflowEngine.handle(eventMessage, mock(ProcessingContext.class));

        // Same-version duplicate: nothing is spawned. The v1 (and this case v2) instance stays untouched.
        verify(workflowExecutionRepository, never()).save(anyString(), any());
    }

    @Test
    void duplicateWorkflowIdStartRequestIsIgnored() {
        String workflowId = "dup-id";

        // Pre-register a running workflow under "dup-id" at version "1.0.0".
        WorkflowExecution existing = mock(WorkflowExecution.class);
        WorkflowState existingState = mock(WorkflowState.class);
        when(existing.state()).thenReturn(existingState);
        when(existingState.workflowDefinitionVersion()).thenReturn("1.0.0");
        workflowExecutionRepository.save(workflowId, () -> existing);
        clearInvocations(workflowExecutionRepository);

        // A configuration that matches the incoming event and resolves to the same id AT THE SAME VERSION.
        // This is the "same-version duplicate" branch of resolveWorkflowIdForNewSpawn — should be rejected.
        WorkflowConfiguration<?> configuration = mock(WorkflowConfiguration.class);
        WorkflowIdProvider idProvider = event -> workflowId;
        when(configuration.workflowIdProvider()).thenReturn(idProvider);
        when(configuration.workflowVersion()).thenReturn("1.0.0");

        QualifiedName eventName = new QualifiedName("OrderPlaced");
        // Stub the version-aware helper that checkAndCreateNewWorkflow actually invokes.
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(eventName))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, configuration)));

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));

        ProcessingContext processingContext = mock(ProcessingContext.class);
        when(processingContext.resources()).thenReturn(Map.of());

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
    @SuppressWarnings("unchecked")
    void workflowStartStoresRestartTokenAsEngineSafePoint() {
        String workflowId = "wf-1";
        QualifiedName eventName = new QualifiedName("OrderPlaced");
        TrackingToken restartToken = token(18);

        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        var workflowContext = mock(WorkflowContext.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        var executionFactory = mock(WorkflowExecutionFactory.class);
        var execution = mock(WorkflowExecution.class);

        when(configuration.workflowIdProvider()).thenReturn(event -> workflowId);
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(contextFactory.createContext(anyMap(), eq(workflowId), any(), eq(configuration))).thenReturn(
                workflowContext);
        when(executionFactory.create(workflowContext)).thenReturn(execution);
        when(execution.restartToken()).thenReturn(restartToken);
        when(execution.workflowId()).thenReturn(workflowId);

        when(workflowConfigurationRegistry.getHighestVersionConfigurations(eventName))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, configuration)));

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));
        when(eventMessage.payload()).thenReturn(Map.of("orderId", workflowId));
        when(eventMessage.payloadAs(any(TypeReference.class))).thenReturn(Map.of("orderId", workflowId));

        workflowEngine.initializeSafePoint(restartToken);
        ProcessingContext processingContext = processingContext(restartToken);

        workflowEngine.handle(eventMessage, processingContext);

        var tokenCaptor = org.mockito.ArgumentCaptor.forClass(TrackingToken.class);
        verify(safePointStore).storeSafePointToken(tokenCaptor.capture());
        assertSameToken(tokenCaptor.getValue(), restartToken);
        verify(execution).onEvent(eventMessage, processingContext);
    }

    @Test
    @SuppressWarnings("unchecked")
    void firstWorkflowStartWithoutPreviousEventTokenDoesNotStoreEngineSafePoint() {
        String workflowId = "wf-1";
        QualifiedName eventName = new QualifiedName("OrderPlaced");
        TrackingToken currentEventToken = token(18);

        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        var workflowContext = mock(WorkflowContext.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        var executionFactory = mock(WorkflowExecutionFactory.class);
        var execution = mock(WorkflowExecution.class);

        when(configuration.workflowIdProvider()).thenReturn(event -> workflowId);
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(contextFactory.createContext(anyMap(), eq(workflowId), any(), eq(configuration))).thenReturn(
                workflowContext);
        when(executionFactory.create(workflowContext)).thenReturn(execution);
        when(execution.restartToken()).thenReturn(null);
        when(execution.workflowId()).thenReturn(workflowId);

        when(workflowConfigurationRegistry.getHighestVersionConfigurations(eventName))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, configuration)));

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));
        when(eventMessage.payloadAs(any(TypeReference.class))).thenReturn(Map.of("orderId", workflowId));

        workflowEngine.handle(eventMessage, processingContext(currentEventToken));

        verify(safePointStore, never()).storeSafePointToken(any());
        verify(execution).onEvent(any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void secondWorkflowStartReceivesPreviousEventTokenAsRestartToken() {
        QualifiedName eventName = new QualifiedName("OrderPlaced");
        TrackingToken firstEventToken = token(18);
        TrackingToken secondEventToken = token(19);

        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        var executionFactory = mock(WorkflowExecutionFactory.class);

        Map<String, Optional<TrackingToken>> restartTokensByWorkflowId = new HashMap<>();
        Map<WorkflowContext, String> workflowIdsByContext = new HashMap<>();
        when(configuration.workflowIdProvider())
                .thenReturn(event -> String.valueOf(event.payloadAs(new TypeReference<Map<String, Object>>() {
                }).get("orderId")));
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(eventName))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, configuration)));

        when(contextFactory.createContext(anyMap(), anyString(), any(), eq(configuration))).thenAnswer(invocation -> {
            String workflowId = invocation.getArgument(1);
            ProcessingContext processingContext = invocation.getArgument(2);
            @SuppressWarnings("unchecked")
            Optional<TrackingToken> restartToken = (Optional<TrackingToken>) processingContext.resources()
                                                                                              .get(RESTART_TOKEN_RESOURCE_KEY);
            restartTokensByWorkflowId.put(workflowId, restartToken);
            var workflowContext = mock(WorkflowContext.class);
            when(workflowContext.processingContext()).thenReturn(processingContext);
            workflowIdsByContext.put(workflowContext, workflowId);
            return workflowContext;
        });
        when(executionFactory.create(any())).thenAnswer(invocation -> {
            WorkflowContext workflowContext = invocation.getArgument(0);
            String workflowId = workflowIdsByContext.get(workflowContext);
            var execution = mock(WorkflowExecution.class);
            when(execution.workflowId()).thenReturn(workflowId);
            when(execution.restartToken()).thenReturn(restartTokensByWorkflowId.get(workflowId).orElse(null));
            when(execution.workflowContext()).thenReturn(workflowContext);
            when(execution.state()).thenReturn(mock(WorkflowState.class));
            return execution;
        });

        workflowEngine.handle(startEvent(eventName, "wf-1"), processingContext(firstEventToken));
        workflowEngine.handle(startEvent(eventName, "wf-2"), processingContext(secondEventToken));

        assertThat(restartTokensByWorkflowId.get("wf-1")).isEqualTo(Optional.empty());
        assertThat(restartTokensByWorkflowId.get("wf-2")).contains(firstEventToken);
    }

    @Test
    @SuppressWarnings("unchecked")
    void replaySpawnsAcrossEventsProduceConsistentRestartTokenTypes() {
        // Reproduces the "Incompatible token type provided: ReplayToken" crash:
        // when replay starts from a previously persisted safePoint and multiple workflows
        // are spawned across replay events, earlier spawns used to receive a raw restart
        // token while later spawns received a ReplayToken — making determineEngineSafePoint
        // crash on GlobalSequenceTrackingToken.lowerBound(ReplayToken).
        QualifiedName eventName = new QualifiedName("OrderPlaced");
        TrackingToken safePoint = token(10);
        TrackingToken tokenAtReset = token(20);
        TrackingToken firstReplayToken = ReplayToken.createReplayToken(tokenAtReset, token(10));
        TrackingToken secondReplayToken = ReplayToken.createReplayToken(tokenAtReset, token(15));

        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        var executionFactory = mock(WorkflowExecutionFactory.class);

        Map<String, TrackingToken> restartTokensByWorkflowId = new HashMap<>();
        Map<WorkflowContext, String> workflowIdsByContext = new HashMap<>();
        when(configuration.workflowIdProvider())
                .thenReturn(event -> String.valueOf(event.payloadAs(new TypeReference<Map<String, Object>>() {
                }).get("orderId")));
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(eventName))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, configuration)));

        when(contextFactory.createContext(anyMap(), anyString(), any(), eq(configuration))).thenAnswer(invocation -> {
            String workflowId = invocation.getArgument(1);
            ProcessingContext processingContext = invocation.getArgument(2);
            Optional<TrackingToken> restartTokenResource = (Optional<TrackingToken>) processingContext.resources()
                                                                                                     .get(RESTART_TOKEN_RESOURCE_KEY);
            restartTokensByWorkflowId.put(workflowId, restartTokenResource.orElse(null));
            var workflowContext = mock(WorkflowContext.class);
            when(workflowContext.processingContext()).thenReturn(processingContext);
            workflowIdsByContext.put(workflowContext, workflowId);
            return workflowContext;
        });
        when(executionFactory.create(any())).thenAnswer(invocation -> {
            WorkflowContext workflowContext = invocation.getArgument(0);
            String workflowId = workflowIdsByContext.get(workflowContext);
            var execution = mock(WorkflowExecution.class);
            var state = mock(WorkflowState.class);
            when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
            when(execution.workflowId()).thenReturn(workflowId);
            when(execution.restartToken()).thenReturn(restartTokensByWorkflowId.get(workflowId));
            when(execution.workflowContext()).thenReturn(workflowContext);
            when(execution.state()).thenReturn(state);
            ProcessingContext workflowProcessingContext = mock(ProcessingContext.class);
            when(workflowContext.processingContext()).thenReturn(workflowProcessingContext);
            when(workflowProcessingContext.whenComplete(any())).thenReturn(workflowProcessingContext);
            return execution;
        });

        workflowEngine.initializeSafePoint(safePoint);

        workflowEngine.handle(startEvent(eventName, "wf-1"), processingContext(firstReplayToken));
        workflowEngine.handle(startEvent(eventName, "wf-2"), processingContext(secondReplayToken));

        // Both restart tokens must be raw (i.e. not ReplayToken-wrapped) so that
        // determineEngineSafePoint can combine them via lowerBound without crashing.
        assertThat(restartTokensByWorkflowId.get("wf-1")).isNotInstanceOf(ReplayToken.class);
        assertThat(restartTokensByWorkflowId.get("wf-2")).isNotInstanceOf(ReplayToken.class);

        ReplayStatusChanged replayFinished = mock(ReplayStatusChanged.class);
        ReplayStatus status = mock(ReplayStatus.class);
        when(replayFinished.status()).thenReturn(status);
        when(status.isReplay()).thenReturn(false);

        // Without the fix, this throws IllegalArgumentException: Incompatible token type provided: ReplayToken.
        workflowEngine.handle(replayFinished, processingContext(secondReplayToken));
    }

    @Test
    void finishingLastWorkflowStoresCurrentTrackingToken() {
        TrackingToken restartToken = token(18);
        TrackingToken currentToken = token(191);

        WorkflowExecution execution = mock(WorkflowExecution.class);
        WorkflowState state = mock(WorkflowState.class);
        WorkflowContext workflowContext = mock(WorkflowContext.class);
        ProcessingContext workflowProcessingContext = mock(ProcessingContext.class);

        when(execution.workflowId()).thenReturn("runningId");
        when(execution.state()).thenReturn(state);
        when(execution.workflowContext()).thenReturn(workflowContext);
        when(execution.restartToken()).thenReturn(restartToken);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(workflowContext.processingContext()).thenReturn(workflowProcessingContext);
        when(workflowProcessingContext.whenComplete(any())).thenAnswer(invocation -> {
            Consumer<ProcessingContext> consumer = invocation.getArgument(0);
            consumer.accept(workflowProcessingContext);
            return workflowProcessingContext;
        });
        doAnswer(invocation -> {
            Consumer<WorkflowExecution> terminationHandler = invocation.getArgument(0);
            terminationHandler.accept(execution);
            return null;
        }).when(execution).execute(any());

        workflowExecutionRepository.save("runningId", () -> execution);

        ReplayStatusChanged replayStatusChanged = mock(ReplayStatusChanged.class);
        ReplayStatus status = mock(ReplayStatus.class);
        when(replayStatusChanged.status()).thenReturn(status);
        when(status.isReplay()).thenReturn(false);

        workflowEngine.handle(replayStatusChanged, processingContext(currentToken));

        var tokenCaptor = org.mockito.ArgumentCaptor.forClass(TrackingToken.class);
        verify(safePointStore, times(2)).storeSafePointToken(tokenCaptor.capture());
        assertSameToken(tokenCaptor.getAllValues().get(0), restartToken);
        assertSameToken(tokenCaptor.getAllValues().get(1), currentToken);
    }

    private static TrackingToken token(long globalIndex) {
        return new GlobalSequenceTrackingToken(globalIndex);
    }

    private static ProcessingContext processingContext(TrackingToken token) {
        ProcessingContext processingContext = mock(ProcessingContext.class);
        Map<Context.ResourceKey<?>, Object> resources = new HashMap<>();
        resources.put(TrackingToken.RESOURCE_KEY, token);
        when(processingContext.resources()).thenReturn(resources);
        doAnswer(invocation -> {
            Context.ResourceKey<?> key = invocation.getArgument(0);
            Object value = invocation.getArgument(1);
            resources.put(key, value);
            return processingContext;
        }).when(processingContext).putResource(any(), any());
        return processingContext;
    }

    private static EventMessage startEvent(QualifiedName eventName, String workflowId) {
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));
        when(eventMessage.payloadAs(any(TypeReference.class))).thenReturn(Map.of("orderId", workflowId));
        return eventMessage;
    }

    private static EventMessage workflowStartedEvent(String workflowId,
                                                     WorkflowDefinitionId definitionId,
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
    void crossVersionStart_disambiguatedIdAlsoTaken_isRejected() {
        // Both the base id and the disambiguated id are already occupied.
        String baseId = "order-1";
        String disambiguatedId = baseId + "#2.0.0";

        WorkflowExecution v1Existing = mock(WorkflowExecution.class);
        WorkflowState v1State = mock(WorkflowState.class);
        when(v1Existing.state()).thenReturn(v1State);
        when(v1State.workflowDefinitionVersion()).thenReturn("1.0.0");
        workflowExecutionRepository.save(baseId, () -> v1Existing);

        WorkflowExecution v2Existing = mock(WorkflowExecution.class);
        WorkflowState v2State = mock(WorkflowState.class);
        when(v2Existing.state()).thenReturn(v2State);
        when(v2State.workflowDefinitionVersion()).thenReturn("2.0.0");
        workflowExecutionRepository.save(disambiguatedId, () -> v2Existing);
        clearInvocations(workflowExecutionRepository);

        WorkflowConfiguration<?> v2 = mock(WorkflowConfiguration.class);
        when(v2.workflowIdProvider()).thenReturn(event -> baseId);
        when(v2.workflowVersion()).thenReturn("2.0.0");

        QualifiedName eventName = new QualifiedName("OrderPlaced");
        when(workflowConfigurationRegistry.getHighestVersionConfigurations(eventName))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, v2)));

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));

        workflowEngine.handle(eventMessage, mock(ProcessingContext.class));

        // Even the disambiguated id is taken — nothing new is spawned.
        verify(workflowExecutionRepository, never()).save(anyString(), any());
    }

    private static void assertSameToken(@Nullable TrackingToken actual, @Nullable TrackingToken expected) {
        assertThat(actual).isNotNull();
        assertThat(expected).isNotNull();
        assertThat(actual.lowerBound(expected)).isEqualTo(expected);
        assertThat(expected.lowerBound(actual)).isEqualTo(actual);
    }

}
