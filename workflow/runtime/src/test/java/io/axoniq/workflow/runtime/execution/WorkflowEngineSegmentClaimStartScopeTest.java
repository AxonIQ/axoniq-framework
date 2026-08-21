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
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.SEGMENT_COUNT;
import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.anotherSegmentThan;
import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.anyIdOn;
import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.owningSegment;
import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.token;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The pass that removes terminal executions and starts the restored ones is scoped by {@link Segment}, at both of its
 * call sites: the claim of a segment, and a segment reaching the replay boundary. Each only concerns the executions
 * that segment owns, the way {@code releaseWorkflowsFor} already does.
 * <p>
 * Without that scoping the predicates select over the whole execution repository, so one segment's claim or catch-up
 * starts the instances of every other segment the node holds: a second, concurrent run of a body that is already
 * running, or a body run on behalf of a segment that has not caught up.
 * <p>
 * Observation channel: the restored execution counts the times the engine starts its body. Nothing in the engine
 * records which segment started which instance, and no engine API is added for it here.
 */
class WorkflowEngineSegmentClaimStartScopeTest {

    private static final String RESIDENT_ID = "sharded-0";
    private static final MessageType DEFINITION_ID =
            new MessageType(new QualifiedName("RestoredWorkflow"), "1.0.0");

    private static final long STARTUP_LATEST_POSITION = 100;
    private static final long LAGGING_POSITION = 7;

    private WorkflowConfigurationRegistry<?> configurationRegistry;
    private WorkflowStore workflowStore;
    private WorkflowEngine workflowEngine;
    private WorkflowEngineReplaySupport replaySupport;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;

    /** Body starts observed for the resident instance, in claim order. */
    private final List<String> bodyStarts = new ArrayList<>();

    @BeforeEach
    void setUp() {
        configurationRegistry = mock(WorkflowConfigurationRegistry.class);
        workflowStore = mock(WorkflowStore.class);
        workflowEngine = new WorkflowEngine(
                configurationRegistry,
                new InMemoryWorkflowExecutionRepository(),
                workflowStore,
                mock(UnitOfWorkFactory.class)
        );
        replaySupport = new WorkflowEngineReplaySupport(workflowEngine);
        checkpointingSupport = new WorkflowEngineCheckpointingSupport(workflowEngine);
        workflowEngine.setEngineSupportComponents(replaySupport, checkpointingSupport);
    }

    @Test
    void claimingASegmentDoesNotStartTheWorkflowExecutionsOwnedByAnotherSegment() {
        var owner = owningSegment(RESIDENT_ID);
        var other = anotherSegmentThan(owner);

        assertThat(SEGMENT_COUNT).as("this oracle is meaningless with a single segment").isGreaterThan(1);
        assertThat(other.getSegmentId()).as("the two segments must differ").isNotEqualTo(owner.getSegmentId());
        assertThat(WorkflowSegmentOwnership.ownedBy(owner, RESIDENT_ID))
                .as("segment %s must own '%s'", owner, RESIDENT_ID).isTrue();
        assertThat(WorkflowSegmentOwnership.ownedBy(other, RESIDENT_ID))
                .as("segment %s must NOT own '%s'", other, RESIDENT_ID).isFalse();

        var restoredState = restoredState();
        registerRestorableWorkflow(restoredState);

        // 1. The node claims the owning segment. The instance is sourced, materialized and started: correct.
        workflowEngine.restoreWorkflowsFor(owner, null, sourcingContext(), new StubProcessingContext()).join();
        assertThat(bodyStarts)
                .as("claiming the owning segment %s starts its own instance", owner)
                .containsExactly(RESIDENT_ID);
        bodyStarts.clear();

        // 2. The coordinator hands the same node a second, unrelated segment.
        workflowEngine.restoreWorkflowsFor(other, null, sourcingContext(), new StubProcessingContext()).join();

        verify(workflowStore, times(1)).loadWorkflow(eq(RESIDENT_ID), any());
        assertThat(workflowEngine.workflowExecutions())
                .as("the instance was resident, and was not re-sourced by the second claim")
                .hasSize(1);

        assertThat(bodyStarts)
                .as("""
                    Workflow bodies started by the claim of segment %s: %s. Expected: none. Instance '%s' is owned by \
                    segment %s, which this claim does not concern, so claiming %s must start nothing (mirroring \
                    releaseWorkflowsFor, which is scoped by ownedBy(segment)). Starting it here gives '%s' a second, \
                    concurrent run of a body that is already running.""",
                    other, bodyStarts, RESIDENT_ID, owner, other, RESIDENT_ID)
                .isEmpty();
    }

    /**
     * The second call site of the same start pass: a segment reaching the replay boundary must start only the
     * executions it owns.
     * <p>
     * An instance restored while its own segment was still replaying stays materialized-but-not-started. When an
     * unrelated segment then catches up, {@code onLiveModeActivated} runs for that segment, and its start pass must
     * not touch the parked instance of the other segment. The owning segment catching up must still start it: the
     * scoping must narrow the pass, not disable it.
     */
    @Test
    void aSegmentReachingTheReplayBoundaryStartsOnlyItsOwnWorkflowExecutions() {
        var owner = owningSegment(RESIDENT_ID);
        var other = anotherSegmentThan(owner);

        assertThat(other.getSegmentId()).as("the two segments must differ").isNotEqualTo(owner.getSegmentId());
        assertThat(WorkflowSegmentOwnership.ownedBy(other, RESIDENT_ID))
                .as("segment %s must NOT own '%s'", other, RESIDENT_ID).isFalse();

        registerRestorableWorkflow(restoredState());
        replaySupport.setInitialEngineTokens(token(0), token(STARTUP_LATEST_POSITION));

        // 1. The owning segment is observed behind the startup latest token, then claimed: materialize, do not start.
        workflowEngine.handle(engineEvent(RESIDENT_ID), processingContext(owner, token(LAGGING_POSITION)));
        workflowEngine.restoreWorkflowsFor(owner,
                                           token(LAGGING_POSITION),
                                           sourcingContext(),
                                           processingContext(owner, null))
                      .join();
        assertThat(replaySupport.isReplaying(owner, token(LAGGING_POSITION)))
                .as("segment %s must still be replaying at position %s", owner, LAGGING_POSITION).isTrue();
        assertThat(workflowEngine.workflowExecutions())
                .as("'%s' must be materialized by the claim", RESIDENT_ID).hasSize(1);
        assertThat(bodyStarts).as("a replaying segment must not start its bodies yet").isEmpty();

        // 2. The unrelated segment reaches the startup latest token and switches itself to live mode.
        workflowEngine.handle(engineEvent(anyIdOn(other)), processingContext(other, token(STARTUP_LATEST_POSITION)));

        assertThat(replaySupport.inLiveMode(other)).as("segment %s must have gone live", other).isTrue();
        assertThat(bodyStarts)
                .as("""
                    Workflow bodies started when segment %s reached the replay boundary: %s. Expected: none. \
                    Instance '%s' is owned by segment %s, which is still replaying at position %s; segment %s going \
                    live concerns only its own instances. Starting '%s' here runs a body on behalf of a segment that \
                    has not caught up, and any completion it produces would be attributed to segment %s's position.""",
                    other, bodyStarts, RESIDENT_ID, owner, LAGGING_POSITION, other, RESIDENT_ID, other)
                .isEmpty();

        // 3. The owning segment catches up: now, and only now, its instance starts.
        workflowEngine.handle(engineEvent(RESIDENT_ID), processingContext(owner, token(STARTUP_LATEST_POSITION)));
        assertThat(bodyStarts)
                .as("segment %s catching up must start the instance it owns", owner)
                .containsExactly(RESIDENT_ID);
    }

    /**
     * A claim is the only signal a node gets for a segment no delivery has reached, and the deferral is lifted
     * exclusively by a delivery on that same segment. Deferring on the claim position alone therefore parks the
     * segment's instances for good whenever the remaining events belong to other segments: their deliveries
     * advance this segment's stored token without ever reaching its handler.
     * <p>
     * So a segment claimed behind the startup latest token, with no delivery ever observed on it, starts its
     * bodies. The sibling case above defers, because a delivery was observed there and can lift it again.
     */
    @Test
    void aSegmentClaimedBehindWithNoObservedDeliveryStartsItsBodies() {
        var owner = owningSegment(RESIDENT_ID);

        registerRestorableWorkflow(restoredState());
        replaySupport.setInitialEngineTokens(token(0), token(STARTUP_LATEST_POSITION));

        assertThat(token(LAGGING_POSITION).covers(token(STARTUP_LATEST_POSITION)))
                .as("the claim position %s must be behind the startup latest token %s",
                    LAGGING_POSITION, STARTUP_LATEST_POSITION)
                .isFalse();
        assertThat(replaySupport.inLiveMode(owner))
                .as("segment %s must not already be live", owner).isFalse();

        // Claimed behind, and no handle(...) has ever run for this segment.
        workflowEngine.restoreWorkflowsFor(owner,
                                           token(LAGGING_POSITION),
                                           sourcingContext(),
                                           processingContext(owner, null))
                      .join();

        assertThat(replaySupport.isReplaying(owner, token(LAGGING_POSITION)))
                .as("Segment %s is claimed at %s, behind the startup latest token %s, but no delivery has been "
                            + "observed on it. Reporting it as replaying defers '%s' with nothing able to lift "
                            + "the deferral: events for other segments advance this segment's stored token "
                            + "without calling its handler.",
                    owner, LAGGING_POSITION, STARTUP_LATEST_POSITION, RESIDENT_ID)
                .isFalse();
        assertThat(bodyStarts)
                .as("'%s' must be started by the claim, not parked until a delivery that never comes", RESIDENT_ID)
                .containsExactly(RESIDENT_ID);
    }

    /**
     * Wires the store and the registry so that {@link RESIDENT_ID} can be rehydrated, and gives the resulting
     * execution the counting body. The execution reports {@code isRunning() == false} throughout: that is the state
     * of an instance whose body has not been started (what {@code restoreWorkflow} leaves behind, since it only calls
     * {@code initializeState}) and of one parked between runs.
     */
    @SuppressWarnings("unchecked")
    private void registerRestorableWorkflow(WorkflowState restoredState) {
        var running = new EventSourcedRunningWorkflows();
        running.evolve(MetadataUtils.create(RESIDENT_ID, WorkflowStatus.STARTED));
        when(workflowStore.loadRunningWorkflows(any())).thenReturn(CompletableFuture.completedFuture(running));
        when(workflowStore.loadWorkflow(eq(RESIDENT_ID), any()))
                .thenReturn(CompletableFuture.completedFuture(restoredState));

        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn(RESIDENT_ID);
        when(execution.state()).thenReturn(restoredState);
        when(execution.isRunning()).thenReturn(false);
        var workflowContext = mock(WorkflowContext.class);
        var bodyContext = mock(ProcessingContext.class);
        when(workflowContext.processingContext()).thenReturn(bodyContext);
        when(bodyContext.whenComplete(any())).thenAnswer(invocation -> {
            invocation.<Consumer<ProcessingContext>>getArgument(0).accept(bodyContext);
            return bodyContext;
        });
        when(execution.workflowContext()).thenReturn(workflowContext);
        doAnswer(invocation -> {
            bodyStarts.add(RESIDENT_ID);
            return null;
        }).when(execution).execute(any());

        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory executionFactory = mock(WorkflowExecutionFactory.class);
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(contextFactory.createContext(anyMap(), eq(RESIDENT_ID), any(), eq(configuration)))
                .thenReturn(workflowContext);
        when(executionFactory.create(workflowContext)).thenReturn(execution);
        when(configurationRegistry.getWorkflowConfiguration(DEFINITION_ID)).thenReturn(Optional.of(configuration));
    }

    private static WorkflowState restoredState() {
        var state = mock(WorkflowState.class);
        when(state.workflowDefinitionId()).thenReturn(DEFINITION_ID);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(state.payload()).thenReturn(Map.of("id", RESIDENT_ID));
        return state;
    }

    private ProcessingContext sourcingContext() {
        return new StubProcessingContext();
    }

    private ProcessingContext processingContext(Segment segment, @Nullable TrackingToken trackingToken) {
        var context = new StubProcessingContext();
        context.putResource(Segment.RESOURCE_KEY, segment);
        if (trackingToken != null) {
            context.putResource(TrackingToken.RESOURCE_KEY, trackingToken);
        }
        return context;
    }

    private static EventMessage engineEvent(String workflowId) {
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.with("workflowId", workflowId));
        when(eventMessage.type()).thenReturn(new MessageType("SomeStepCompleted"));
        return eventMessage;
    }
}
