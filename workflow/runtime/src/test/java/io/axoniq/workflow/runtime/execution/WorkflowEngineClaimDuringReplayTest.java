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
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.owningSegment;
import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.token;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Claiming a segment sources its instances from the event store <em>head</em>, while the segment's own processor
 * position can be far behind it. Starting such an instance's body immediately makes it act on the state at the end of
 * the stream while the events between the segment's position and that head are still being delivered to it: live side
 * effects during replay, and every delivered event applied to an already-advanced instance.
 * <p>
 * The claim must therefore materialize the instance and leave the body parked until the claimed segment itself catches
 * up. The last step asserts the other half of that: the deferral really is lifted by the segment reaching the startup
 * latest token, so a deferred instance is not parked forever.
 */
class WorkflowEngineClaimDuringReplayTest {

    private static final String RESIDENT_ID = "sharded-0";
    private static final MessageType DEFINITION_ID =
            new MessageType(new QualifiedName("RestoredWorkflow"), "1.0.0");

    private static final long STARTUP_LATEST_POSITION = 100;
    private static final long SEGMENT_POSITION_BEFORE_RELEASE = 7;

    private WorkflowConfigurationRegistry<?> configurationRegistry;
    private WorkflowStore workflowStore;
    private WorkflowEngine workflowEngine;
    private WorkflowEngineReplaySupport replaySupport;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;

    /**
     * Body starts observed for the resident instance, in order. The observation channel.
     */
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
        registerRestorableWorkflow();
    }

    @Test
    void reclaimingASegmentThatIsStillReplayingDoesNotRunTheRestoredBodyAtHeadState() {
        var owner = owningSegment(RESIDENT_ID);

        assertThat(WorkflowSegmentOwnership.ownedBy(owner, RESIDENT_ID))
                .as("segment %s must own '%s'", owner, RESIDENT_ID).isTrue();
        assertThat(token(SEGMENT_POSITION_BEFORE_RELEASE).covers(token(STARTUP_LATEST_POSITION)))
                .as("the segment at %s must NOT have reached the startup latest token %s",
                    SEGMENT_POSITION_BEFORE_RELEASE, STARTUP_LATEST_POSITION)
                .isFalse();

        replaySupport.setInitialEngineTokens(token(0), token(STARTUP_LATEST_POSITION));

        // 1. The node holds the segment and delivers one event on it, leaving it at position 7 of a stream whose
        //    startup latest token is 100.
        workflowEngine.restoreWorkflowsFor(owner, token(0), sourcingContext(), executionContext()).join();
        workflowEngine.handle(engineEvent(RESIDENT_ID),
                              deliveryContext(owner, token(SEGMENT_POSITION_BEFORE_RELEASE)));
        bodyStarts.clear();

        // 2. The segment is handed away and comes back, still at position 7.
        workflowEngine.releaseWorkflowsFor(owner);
        workflowEngine.restoreWorkflowsFor(owner,
                                           token(SEGMENT_POSITION_BEFORE_RELEASE),
                                           sourcingContext(),
                                           executionContext())
                      .join();

        assertThat(workflowEngine.workflowExecutions())
                .as("the re-claim must still materialize the instance, it only defers running its body")
                .hasSize(1);

        assertThat(bodyStarts)
                .as("""
                            Workflow bodies started by the re-claim of segment %s: %s. Expected: none. The claim sourced \
                            '%s' from the event-store head while segment %s sits at position %s and the startup latest token \
                            is %s, so positions %s..%s are still to be delivered to it. A body started here runs at head \
                            state and emits live side effects while those events replay onto it.""",
                    owner, bodyStarts, RESIDENT_ID, owner, SEGMENT_POSITION_BEFORE_RELEASE, STARTUP_LATEST_POSITION,
                    SEGMENT_POSITION_BEFORE_RELEASE + 1, STARTUP_LATEST_POSITION)
                .isEmpty();

        // 3. The segment catches up. The deferral must be lifted, or the instance is parked forever.
        workflowEngine.handle(engineEvent(RESIDENT_ID), deliveryContext(owner, token(STARTUP_LATEST_POSITION)));

        assertThat(bodyStarts)
                .as("reaching the startup latest token must start the instances the claim deferred")
                .containsExactly(RESIDENT_ID);
    }

    /**
     * Wires the store and the registry so {@link #RESIDENT_ID} can be rehydrated, and gives the resulting execution the
     * counting body. The execution reports {@code isRunning() == false} throughout, the state of an instance whose body
     * has not been started, so nothing but the engine's own gate can keep it from being started.
     */
    private void registerRestorableWorkflow() {
        var restoredState = mock(WorkflowState.class);
        when(restoredState.workflowDefinitionId()).thenReturn(DEFINITION_ID);
        when(restoredState.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(restoredState.payload()).thenReturn(Map.of("id", RESIDENT_ID));

        var running = new EventSourcedRunningWorkflows();
        running.evolve(MetadataUtils.create(RESIDENT_ID, WorkflowStatus.STARTED));
        when(workflowStore.loadRunningWorkflows(any())).thenReturn(CompletableFuture.completedFuture(running));
        when(workflowStore.loadWorkflow(eq(RESIDENT_ID), any()))
                .thenReturn(CompletableFuture.completedFuture(restoredState));

        var execution = WorkflowExecutionFixture.mockExecution(RESIDENT_ID, restoredState, false);
        WorkflowExecutionFixture.recordBodyStartOn(execution, bodyStarts::add, RESIDENT_ID);
        var configuration = WorkflowExecutionFixture.mockConfiguration(RESIDENT_ID, execution);
        when(configurationRegistry.getWorkflowConfiguration(DEFINITION_ID)).thenReturn(Optional.of(configuration));
    }

    private ProcessingContext sourcingContext() {
        return new StubProcessingContext();
    }

    private ProcessingContext executionContext() {
        return new StubProcessingContext();
    }

    /**
     * A processor batch context carrying the segment the event is delivered under, and its position.
     */
    private ProcessingContext deliveryContext(Segment segment, @Nullable TrackingToken trackingToken) {
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
