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
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry.PredicatedWorkflowConfiguration;
import jakarta.annotation.Nullable;
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.FOUR_SEGMENTS;
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
import static org.mockito.Mockito.when;

/**
 * {@link WorkflowEngineReplaySupport} keys its replay state by {@link Segment}: a live-mode flag and a current position
 * per segment. With more than one segment the segments catch up at different times, so:
 * <ul>
 *     <li>a segment reaching the startup latest token switches only itself to live mode, and a workflow body owned by
 *     a segment that is still replaying is materialized without being started;</li>
 *     <li>a restored execution's context is seeded with its own segment's position, not with whatever token the last
 *     event on any other segment carried.</li>
 * </ul>
 * Both are asserted on an observable effect: a counted workflow-body start, and the token actually placed in the
 * restore context.
 */
class WorkflowEngineSegmentLiveModeScopeTest {

    private static final QualifiedName START_EVENT = new QualifiedName("StartShardedWorkflow");

    private static final long STARTUP_LATEST_POSITION = 100;
    private static final long CAUGHT_UP_SEGMENT_POSITION = 100;
    private static final long LAGGING_SEGMENT_POSITION = 7;

    private WorkflowConfigurationRegistry<?> configurationRegistry;
    private WorkflowEngine workflowEngine;
    private WorkflowEngineReplaySupport replaySupport;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;

    /** Workflow bodies the engine started, in order. The observation channel. */
    private final List<String> bodyStarts = new ArrayList<>();

    @BeforeEach
    void setUp() {
        configurationRegistry = mock(WorkflowConfigurationRegistry.class);
        workflowEngine = new WorkflowEngine(
                configurationRegistry,
                new InMemoryWorkflowExecutionRepository(),
                mock(WorkflowStore.class),
                mock(UnitOfWorkFactory.class)
        );
        replaySupport = new WorkflowEngineReplaySupport(workflowEngine);
        checkpointingSupport = new WorkflowEngineCheckpointingSupport(workflowEngine);
        workflowEngine.setEngineSupportComponents(replaySupport, checkpointingSupport);
    }

    @Test
    void noWorkflowBodyRunsOnASegmentThatHasNotReachedTheStartupLatestToken() {
        var startId = "sharded-0";
        var lagging = owningSegment(startId);
        var caughtUp = anotherSegmentThan(lagging);
        var caughtUpId = anyIdOn(caughtUp);

        // --- precondition evidence -------------------------------------------------------------------------------
        assertThat(SEGMENT_COUNT).as("this oracle is meaningless with a single segment").isGreaterThan(1);
        assertThat(caughtUp.getSegmentId()).as("the two segments must differ").isNotEqualTo(lagging.getSegmentId());
        assertThat(WorkflowSegmentOwnership.ownedBy(lagging, startId))
                .as("the start '%s' must be owned by the lagging segment %s", startId, lagging).isTrue();
        assertThat(token(LAGGING_SEGMENT_POSITION).covers(token(STARTUP_LATEST_POSITION)))
                .as("the lagging segment at %s must NOT have reached the startup latest token %s",
                    LAGGING_SEGMENT_POSITION, STARTUP_LATEST_POSITION)
                .isFalse();

        registerStartConfiguration(startId);
        replaySupport.setInitialEngineTokens(token(0), token(STARTUP_LATEST_POSITION));
        assertThat(replaySupport.inLiveMode()).as("the engine starts in replay mode").isFalse();

        // 1. Only the caught-up segment reaches the startup latest token.
        workflowEngine.handle(engineEvent(caughtUpId),
                              processingContext(caughtUp, token(CAUGHT_UP_SEGMENT_POSITION)));

        // 2. The lagging segment, still at position 7, handles a start event for an instance it owns.
        workflowEngine.handle(startEvent(startId), processingContext(lagging, token(LAGGING_SEGMENT_POSITION)));

        // --- oracle ----------------------------------------------------------------------------------------------
        assertThat(bodyStarts)
                .as("""
                    Workflow bodies started while their own segment is still replaying: %s. Expected: none. Segment \
                    %s is at position %s and the startup latest token is %s, so it is still replaying; the start of \
                    '%s' must be materialized without running its body until that segment catches up. Segment %s \
                    reaching %s switches only itself to live mode. A body started here would run against a partially \
                    replayed instance view and emit live side effects during replay.""",
                    bodyStarts, lagging, LAGGING_SEGMENT_POSITION, STARTUP_LATEST_POSITION, startId,
                    caughtUp, CAUGHT_UP_SEGMENT_POSITION)
                .isEmpty();
    }

    @Test
    void nodeStartingWithoutSegmentsStillDefersBodiesOnALaterClaimedLaggingSegment() {
        var startId = "sharded-0";
        var lagging = owningSegment(startId);

        // --- precondition evidence -------------------------------------------------------------------------------
        assertThat(token(LAGGING_SEGMENT_POSITION).covers(token(STARTUP_LATEST_POSITION)))
                .as("the lagging segment at %s must NOT have reached the startup latest token %s",
                    LAGGING_SEGMENT_POSITION, STARTUP_LATEST_POSITION)
                .isFalse();

        registerStartConfiguration(startId);
        // A node that owns zero segments at startup: earliestSegmentToken() is null, so requiresReplay() is false
        // and WorkflowEngine#start switches to live mode with a context that carries no segment.
        replaySupport.setInitialEngineTokens(null, token(STARTUP_LATEST_POSITION));
        replaySupport.switchToLiveMode(contextWithoutSegment());

        // Later this node claims the lagging segment and its first delivery is an old event at position 7.
        workflowEngine.handle(startEvent(startId), processingContext(lagging, token(LAGGING_SEGMENT_POSITION)));

        // --- oracle ----------------------------------------------------------------------------------------------
        assertThat(replaySupport.isReplaying(lagging, token(LAGGING_SEGMENT_POSITION)))
                .as("""
                    Segment %s was observed at position %s, behind the startup latest token %s, so it is replaying. \
                    The engine-wide live-mode flag of a node that started owning nothing must not mask that.""",
                    lagging, LAGGING_SEGMENT_POSITION, STARTUP_LATEST_POSITION)
                .isTrue();
        assertThat(bodyStarts)
                .as("""
                    Workflow bodies started on a still-replaying segment: %s. Expected: none. The node started \
                    owning zero segments, which set the engine-wide live-mode flag; that flag must not make a \
                    later-claimed lagging segment run bodies at head state during its catch-up.""",
                    bodyStarts)
                .isEmpty();
    }

    @Test
    void aRestoredExecutionIsSeededWithTheTokenOfItsOwnSegment() {
        var lagging = FOUR_SEGMENTS.get(0);
        var caughtUp = anotherSegmentThan(lagging);
        var laggingId = anyIdOn(lagging);
        var caughtUpId = anyIdOn(caughtUp);

        // --- precondition evidence -------------------------------------------------------------------------------
        assertThat(caughtUp.getSegmentId()).as("the two segments must differ").isNotEqualTo(lagging.getSegmentId());
        assertThat(WorkflowSegmentOwnership.ownedBy(lagging, laggingId)).isTrue();
        assertThat(WorkflowSegmentOwnership.ownedBy(caughtUp, caughtUpId)).isTrue();

        replaySupport.setInitialEngineTokens(token(0), token(STARTUP_LATEST_POSITION));

        // The lagging segment is at position 7; the other segment then races ahead to position 100.
        workflowEngine.handle(engineEvent(laggingId), processingContext(lagging, token(LAGGING_SEGMENT_POSITION)));
        workflowEngine.handle(engineEvent(caughtUpId),
                              processingContext(caughtUp, token(CAUGHT_UP_SEGMENT_POSITION)));

        // The lagging segment is (re)claimed and its instances are restored into a fresh execution context.
        var sourcingContext = processingContext(lagging, null);
        var executionContext = processingContext(lagging, null);
        var workflowStore = mock(WorkflowStore.class);
        when(workflowStore.loadRunningWorkflows(any()))
                .thenReturn(CompletableFuture.completedFuture(new EventSourcedRunningWorkflows()));
        // Shares the replay support the positions above were observed by: a claim reads the position of its segment
        // from the same support the processor callbacks fed.
        var claimingEngine = new WorkflowEngine(configurationRegistry,
                                                new InMemoryWorkflowExecutionRepository(),
                                                workflowStore,
                                                mock(UnitOfWorkFactory.class));
        claimingEngine.setEngineSupportComponents(replaySupport, checkpointingSupport);
        claimingEngine.restoreWorkflowsFor(lagging, token(LAGGING_SEGMENT_POSITION), sourcingContext, executionContext);

        // --- oracle ----------------------------------------------------------------------------------------------
        var seeded = executionContext.resources().get(TrackingToken.RESOURCE_KEY);
        assertThat(seeded)
                .as("""
                    Tracking token seeded into the restore context of segment %s: %s. Expected: %s, that segment's \
                    own position, not %s, the position of segment %s. A restored instance seeded with another \
                    segment's position is told it resumes from a position its own segment never reached.""",
                    lagging, seeded, token(LAGGING_SEGMENT_POSITION), token(CAUGHT_UP_SEGMENT_POSITION), caughtUp)
                .isEqualTo(token(LAGGING_SEGMENT_POSITION));
    }

    // ---------------------------------------------------------------------------------------------------------
    // rig
    // ---------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private void registerStartConfiguration(String workflowId) {
        var execution = mock(WorkflowExecution.class);
        var state = mock(WorkflowState.class);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(execution.workflowId()).thenReturn(workflowId);
        when(execution.state()).thenReturn(state);
        var workflowContext = mock(WorkflowContext.class);
        var bodyContext = mock(ProcessingContext.class);
        when(workflowContext.processingContext()).thenReturn(bodyContext);
        when(bodyContext.whenComplete(any())).thenAnswer(invocation -> {
            invocation.<Consumer<ProcessingContext>>getArgument(0).accept(bodyContext);
            return bodyContext;
        });
        when(execution.workflowContext()).thenReturn(workflowContext);
        doAnswer(invocation -> {
            bodyStarts.add(workflowId);
            return null;
        }).when(execution).execute(any());

        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory executionFactory = mock(WorkflowExecutionFactory.class);
        when(configuration.workflowIdProvider()).thenReturn(event -> workflowId);
        when(configuration.workflowVersion()).thenReturn("1.0.0");
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(contextFactory.createContext(anyMap(), eq(workflowId), any(), eq(configuration)))
                .thenReturn(workflowContext);
        when(executionFactory.create(workflowContext)).thenReturn(execution);
        when(configurationRegistry.getHighestVersionConfigurations(new MessageType(START_EVENT)))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, configuration)));
    }

    private ProcessingContext contextWithoutSegment() {
        return processingContext(null, null);
    }

    private ProcessingContext processingContext(@Nullable Segment segment, @Nullable TrackingToken trackingToken) {
        Map<Context.ResourceKey<?>, Object> resources = new HashMap<>();
        if (segment != null) {
            resources.put(Segment.RESOURCE_KEY, segment);
        }
        if (trackingToken != null) {
            resources.put(TrackingToken.RESOURCE_KEY, trackingToken);
        }
        var context = mock(ProcessingContext.class);
        when(context.resources()).thenReturn(resources);
        doAnswer(invocation -> resources.get(invocation.<Context.ResourceKey<?>>getArgument(0)))
                .when(context).getResource(any());
        doAnswer(invocation -> {
            resources.put(invocation.getArgument(0), invocation.getArgument(1));
            return context;
        }).when(context).putResource(any(), any());
        when(context.component(WorkflowEngineReplaySupport.class)).thenReturn(replaySupport);
        when(context.component(WorkflowEngineCheckpointingSupport.class)).thenReturn(checkpointingSupport);
        return context;
    }

    private static EventMessage startEvent(String workflowId) {
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(START_EVENT));
        when(eventMessage.payloadAs(any(TypeReference.class))).thenReturn(Map.of("id", workflowId));
        return eventMessage;
    }

    private static EventMessage engineEvent(String workflowId) {
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.with("workflowId", workflowId));
        when(eventMessage.type()).thenReturn(new MessageType("SomeStepCompleted"));
        return eventMessage;
    }
}
