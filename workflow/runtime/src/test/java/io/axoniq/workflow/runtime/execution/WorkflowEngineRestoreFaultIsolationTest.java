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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Rehydration on a segment claim must resolve the same set of definitions the replay path resolves, and one instance
 * that cannot be resolved at all must not take the segment down with it.
 * <p>
 * A body may move its recorded version forward with {@code ctx.migrateVersion(...)} to a version that no definition is
 * statically registered under. The replay path routes such an instance to the closest registered sibling; rehydration
 * used to look the version up exactly and throw when it did not match.
 * <p>
 * That throw was not contained either: the restores of a segment were joined with {@code allOf}, so a single failure
 * aborted the whole pass. The claim callback then failed, which the processor only logs, leaving every other instance
 * of that segment unrehydrated on a node that still reports healthy.
 */
class WorkflowEngineRestoreFaultIsolationTest {

    /** One segment owning every workflow id, so both instances below land in the same restore pass. */
    private static final Segment ONLY_SEGMENT = new Segment(0, 0);

    private static final String MIGRATED_ID = "migrated-instance";
    private static final String HEALTHY_ID = "healthy-instance";
    private static final String UNRESOLVABLE_ID = "unresolvable-instance";

    private static final String MIGRATING_WORKFLOW = "MigratingWorkflow";
    private static final String HEALTHY_WORKFLOW = "HealthyWorkflow";
    /** No definition is registered under this name at all: not exactly, not lower, not higher. */
    private static final String RETIRED_WORKFLOW = "RetiredWorkflow";

    private SimpleWorkflowConfigurationRegistry configurationRegistry;
    private WorkflowStore workflowStore;
    private WorkflowEngine workflowEngine;
    private WorkflowEngineReplaySupport replaySupport;

    /** Ids whose body the engine started, in restore order. */
    private final List<String> bodyStarts = new ArrayList<>();
    private final EventSourcedRunningWorkflows runningWorkflows = new EventSourcedRunningWorkflows();
    private final Map<String, WorkflowState> statesById = new HashMap<>();

    @BeforeEach
    void setUp() {
        configurationRegistry = new SimpleWorkflowConfigurationRegistry();
        workflowStore = mock(WorkflowStore.class);
        workflowEngine = new WorkflowEngine(
                configurationRegistry,
                new InMemoryWorkflowExecutionRepository(),
                workflowStore,
                mock(UnitOfWorkFactory.class)
        );
        replaySupport = new WorkflowEngineReplaySupport(workflowEngine);
        workflowEngine.setEngineSupportComponents(
                replaySupport, new WorkflowEngineCheckpointingSupport(workflowEngine));
        when(workflowStore.loadRunningWorkflows(any()))
                .thenReturn(CompletableFuture.completedFuture(runningWorkflows));
    }

    /**
     * Fix (a): the recorded version is above every registered one, exactly what two forward
     * {@code ctx.migrateVersion(...)} bumps produce. Rehydration must route to the closest registered sibling, the way
     * {@code resolveDefinitionForReplay} does, instead of failing the lookup.
     */
    @Test
    void restoresAnInstanceWhoseRecordedVersionWasMigratedPastEveryRegisteredVersion() {
        registerDefinition(MIGRATING_WORKFLOW, "2.0.0");
        var state = restorable(MIGRATED_ID, MIGRATING_WORKFLOW, "2.2.0");

        // --- precondition evidence -------------------------------------------------------------------------------
        assertThat(configurationRegistry.getWorkflowConfiguration(state.workflowDefinitionId()))
                .as("no definition is registered at the recorded version 2.2.0, so an exact lookup cannot restore it")
                .isEmpty();
        assertThat(configurationRegistry.findClosestRegisteredVersion(MIGRATING_WORKFLOW, "2.2.0"))
                .as("the replay path can resolve it, through the closest registered sibling")
                .isPresent();

        workflowEngine.restoreWorkflowsFor(ONLY_SEGMENT, null, sourcingContext(), mock(ProcessingContext.class));

        // --- oracle ----------------------------------------------------------------------------------------------
        assertThat(bodyStarts)
                .as("""
                    Instance '%s' recorded version 2.2.0 after two ctx.migrateVersion bumps, and only 2.0.0 is \
                    registered. Rehydration must route it to the closest registered sibling, as the replay path does. \
                    Failing the lookup strands a live workflow that the engine explicitly supports.""",
                    MIGRATED_ID)
                .containsExactly(MIGRATED_ID);
    }

    /**
     * Fix (b): an instance that cannot be resolved at all is skipped, and the rest of the segment is restored anyway.
     * <p>
     * Before the fix the restores were joined with {@code allOf}, so the unresolvable instance failed the claim and no
     * instance of the segment was ever rehydrated.
     */
    @Test
    void oneUnrestorableInstanceDoesNotStopTheRestOfTheSegmentFromBeingRestored() {
        registerDefinition(HEALTHY_WORKFLOW, "1.0.0");
        restorable(HEALTHY_ID, HEALTHY_WORKFLOW, "1.0.0");
        restorable(UNRESOLVABLE_ID, RETIRED_WORKFLOW, "1.0.0");

        // --- precondition evidence -------------------------------------------------------------------------------
        assertThat(configurationRegistry.findClosestRegisteredVersion(RETIRED_WORKFLOW, "1.0.0"))
                .as("'%s' has no registered definition below or at its version", RETIRED_WORKFLOW).isEmpty();
        assertThat(configurationRegistry.findClosestHigherRegisteredVersion(RETIRED_WORKFLOW, "1.0.0"))
                .as("'%s' has no registered definition above its version either", RETIRED_WORKFLOW).isEmpty();
        assertThat(WorkflowSegmentOwnership.ownedBy(ONLY_SEGMENT, HEALTHY_ID)
                           && WorkflowSegmentOwnership.ownedBy(ONLY_SEGMENT, UNRESOLVABLE_ID))
                .as("both instances must be restored by the same segment claim for this oracle to mean anything")
                .isTrue();

        // --- oracle ----------------------------------------------------------------------------------------------
        assertThatCode(() -> workflowEngine.restoreWorkflowsFor(ONLY_SEGMENT,
                                                         null,
                                                         sourcingContext(),
                                                         mock(ProcessingContext.class)))
                .as("""
                    The claim of segment %s must survive an instance it cannot restore. Letting it fail aborts the \
                    restore pass of the whole segment; the processor only logs that, so the node keeps the segment \
                    while none of its instances is ever rehydrated.""", ONLY_SEGMENT)
                .doesNotThrowAnyException();

        assertThat(bodyStarts)
                .as("""
                    Bodies started by the claim of segment %s: %s. Instance '%s' cannot be resolved to a definition \
                    and is skipped, but '%s' can and must be running.""",
                    ONLY_SEGMENT, bodyStarts, UNRESOLVABLE_ID, HEALTHY_ID)
                .containsExactly(HEALTHY_ID);
        assertThat(workflowEngine.workflowExecutions())
                .as("the skipped instance is not materialized; it keeps its durable state for a later claim")
                .hasSize(1);
    }

    // ---------------------------------------------------------------------------------------------------------
    // rig
    // ---------------------------------------------------------------------------------------------------------

    /** Registers a definition whose restored execution records its own body start. */
    @SuppressWarnings("unchecked")
    private void registerDefinition(String workflowName, String workflowVersion) {
        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        when(configuration.workflowName()).thenReturn(workflowName);
        when(configuration.workflowVersion()).thenReturn(workflowVersion);

        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory executionFactory = mock(WorkflowExecutionFactory.class);
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        doAnswer(contextInvocation -> {
            var workflowId = contextInvocation.<String>getArgument(1);
            var workflowContext = mock(WorkflowContext.class);
            var bodyContext = mock(ProcessingContext.class);
            when(workflowContext.processingContext()).thenReturn(bodyContext);
            when(bodyContext.whenComplete(any())).thenAnswer(invocation -> {
                invocation.<Consumer<ProcessingContext>>getArgument(0).accept(bodyContext);
                return bodyContext;
            });
            var execution = mock(WorkflowExecution.class);
            when(execution.workflowId()).thenReturn(workflowId);
            when(execution.state()).thenReturn(stateOf(workflowId));
            when(execution.isRunning()).thenReturn(false);
            when(execution.workflowContext()).thenReturn(workflowContext);
            doAnswer(started -> bodyStarts.add(workflowId)).when(execution).execute(any());
            when(executionFactory.create(workflowContext)).thenReturn(execution);
            return workflowContext;
        }).when(contextFactory).createContext(anyMap(), any(), any(), eq(configuration));

        configurationRegistry.register(new QualifiedName(workflowName + "Started"), configuration);
    }

    /** Makes {@code workflowId} a running workflow whose durable state records the given definition. */
    private WorkflowState restorable(String workflowId, String workflowName, String recordedVersion) {
        var state = mock(WorkflowState.class);
        when(state.workflowDefinitionId())
                .thenReturn(new MessageType(new QualifiedName(workflowName), recordedVersion));
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(state.payload()).thenReturn(Map.of("id", workflowId));
        statesById.put(workflowId, state);
        runningWorkflows.evolve(MetadataUtils.create(workflowId, WorkflowStatus.STARTED));
        when(workflowStore.loadWorkflow(eq(workflowId), any()))
                .thenReturn(CompletableFuture.completedFuture(state));
        return state;
    }

    private WorkflowState stateOf(String workflowId) {
        return statesById.get(workflowId);
    }

    private ProcessingContext sourcingContext() {
        var context = mock(ProcessingContext.class);
        when(context.component(WorkflowEngineReplaySupport.class)).thenReturn(replaySupport);
        return context;
    }
}
