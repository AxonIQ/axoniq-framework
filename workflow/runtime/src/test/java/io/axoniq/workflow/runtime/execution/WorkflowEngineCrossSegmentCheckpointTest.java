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
import io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing;
import io.axoniq.framework.messaging.eventstreaming.checkpoint.CheckpointingProgressStrategy;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry.PredicatedWorkflowConfiguration;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.progress.SegmentProgressContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Pins that a checkpoint is scoped to the segment that asked for it: {@link WorkflowEngineCheckpointingSupport} retains
 * the {@code CheckpointTrigger} of each claimed segment separately, and the engine routes every request to the segment
 * that produced it, so no segment's stored token is ever advanced by another segment's progress.
 * <p>
 * Behaviour measured here: an asynchronous checkpoint request raised by a workflow owned by segment A must not be
 * delivered through the trigger of segment B. If it were, <em>B's</em> stored token would jump to A's stream position
 * and every event between B's real position and A's position would never be redelivered to B after a restart: silent
 * event loss.
 * <p>
 * The rig is deliberately the real machinery on both sides of the boundary: the real framework
 * {@link CheckpointingProgressStrategy} (one per segment, exactly as a work package owns one) drives the engine's real
 * {@link Checkpointing} implementation, and the resulting token is stored in a real {@link InMemoryTokenStore} and read
 * back from it. Only {@link SegmentProgressContext} is a test double: it is the thin work-package seam (segment
 * identity, last consumed position, monotonic store) and it mirrors {@code WorkPackage#storeIfAdvanced}. An in-memory
 * token store is enough because this oracle needs stored
 * <em>positions</em> only, never claim ownership.
 * <p>
 * The framework does not defend against this on the caller's behalf: a requested position is neither clamped to the
 * segment's last consumed token when it is requested nor when it is stored, so an over-high request is persisted
 * verbatim. Keeping the trigger per segment is therefore the only thing standing between a mis-scoped request and lost
 * events.
 */
class WorkflowEngineCrossSegmentCheckpointTest {

    private static final String PROCESSOR = "Workflow";

    /**
     * Instance whose start event segment A handles, and whose asynchronous completion raises the leaking request.
     */
    private static final String STRAGGLER_ID = "sharded-0";
    private static final QualifiedName START_EVENT = new QualifiedName("StartShardedWorkflow");

    /**
     * Stream positions of the four events this test places on the (shared) event stream.
     */
    private static final long SLOW_SEGMENT_HANDLED_POSITION = 5;
    private static final long SLOW_SEGMENT_UNHANDLED_POSITION = 42;
    private static final long FAST_SEGMENT_HANDLED_POSITION = 100;

    private InMemoryTokenStore tokenStore;
    private WorkflowConfigurationRegistry<?> configurationRegistry;
    private WorkflowExecutionRepository repository;
    private WorkflowEngine workflowEngine;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;

    private final AtomicReference<Consumer<WorkflowExecution>> stragglerTermination = new AtomicReference<>();

    @BeforeEach
    void setUp() {
        tokenStore = new InMemoryTokenStore();
        tokenStore.initializeTokenSegments(PROCESSOR, SEGMENT_COUNT, null, null).join();
        configurationRegistry = mock(WorkflowConfigurationRegistry.class);
        repository = new InMemoryWorkflowExecutionRepository();
        workflowEngine = new WorkflowEngine(configurationRegistry,
                                            repository,
                                            mock(WorkflowCancellationService.class),
                                            mock(WorkflowStore.class),
                                            mock(UnitOfWorkFactory.class));
        checkpointingSupport = new WorkflowEngineCheckpointingSupport(workflowEngine);
        workflowEngine.setCheckpointingSupport(checkpointingSupport);
    }

    /**
     * Runs under both processor modes so the result cannot be blamed on the mode: fully-deferred (every handler is
     * {@code Checkpointing}) and auto (an ordinary handler such as the workflow history projector is co-located, which
     * is the production wiring of {@code WorkflowEventProcessingRegistrationEnhancer}).
     */
    @ParameterizedTest(name = "autoCheckpointing={0}")
    @ValueSource(booleans = {false, true})
    void asyncCheckpointOfOneSegmentLeavesTheStoredTokenOfAnotherSegmentAtItsOwnPosition(boolean autoCheckpointing) {
        var fast = owningSegment(STRAGGLER_ID);
        var slowSegmentId = idOnAnotherSegmentThan(STRAGGLER_ID);
        var slow = owningSegment(slowSegmentId);
        var lostId = anotherIdOn(slow, slowSegmentId);

        assertThat(SEGMENT_COUNT).as("this oracle is meaningless with a single segment").isGreaterThan(1);
        assertThat(slow.getSegmentId()).as("the two segments must differ").isNotEqualTo(fast.getSegmentId());
        assertThat(WorkflowSegmentOwnership.ownedBy(fast, STRAGGLER_ID)).isTrue();
        assertThat(WorkflowSegmentOwnership.ownedBy(slow, STRAGGLER_ID)).isFalse();
        assertThat(WorkflowSegmentOwnership.ownedBy(slow, lostId))
                .as("the event at position %s must be owned by the slow segment", SLOW_SEGMENT_UNHANDLED_POSITION)
                .isTrue();

        var fastWorker = new SegmentWorker(fast, autoCheckpointing);
        var slowWorker = new SegmentWorker(slow, autoCheckpointing);
        var straggler = registerStartConfiguration();

        // 1. The node claims the fast segment and handles the straggler's start event at position 100.
        fastWorker.claim();
        fastWorker.handle(startEvent(), FAST_SEGMENT_HANDLED_POSITION);
        fastWorker.commit();
        assertThat(storedToken(fast)).as("the fast segment checkpoints its own position").isEqualTo(token(
                FAST_SEGMENT_HANDLED_POSITION));

        // 2. The coordinator hands the same node the slow segment. Its trigger is retained next to the fast segment's,
        //    not in place of it.
        slowWorker.claim();

        // 3. The straggler workflow (owned by the FAST segment) completes asynchronously. The engine's completion
        //    callback requests a checkpoint at the fast segment's position 100, through the fast segment's trigger.
        assertThat(stragglerTermination.get()).as("the straggler workflow body must have been started").isNotNull();
        stragglerTermination.get().accept(straggler);

        // 4. The slow segment handles one event of its own at position 5 and commits.
        slowWorker.handle(engineEvent(slowSegmentId), SLOW_SEGMENT_HANDLED_POSITION);
        slowWorker.commit();

        assertThat(slowWorker.lastConsumedToken())
                .as("the slow segment really only ever consumed up to position %s", SLOW_SEGMENT_HANDLED_POSITION)
                .isEqualTo(token(SLOW_SEGMENT_HANDLED_POSITION));
        assertThat(slowWorker.handledPositions)
                .as("the slow segment never handled the event at position %s", SLOW_SEGMENT_UNHANDLED_POSITION)
                .doesNotContain(SLOW_SEGMENT_UNHANDLED_POSITION);

        var slowStored = storedToken(slow);
        assertThat(slowStored)
                .as("""
                            Stored token of segment %s, read back from the token store: %s.
                            Expected: %s, this segment's own last consumed position, so the event at position %s for instance \
                            '%s' -- owned by this segment and never handled by it -- is still redelivered after a restart.
                            A regression shows up as %s, the FAST segment's position, pushed through this segment's trigger \
                            because WorkflowEngineCheckpointingSupport stopped keeping a CheckpointTrigger per segment. \
                            Positions %s..%s of this segment would then be silently skipped after a restart.""",
                    slow,
                    slowStored,
                    token(SLOW_SEGMENT_HANDLED_POSITION),
                    SLOW_SEGMENT_UNHANDLED_POSITION,
                    lostId,
                    token(FAST_SEGMENT_HANDLED_POSITION),
                    SLOW_SEGMENT_HANDLED_POSITION + 1,
                    FAST_SEGMENT_HANDLED_POSITION)
                .isEqualTo(token(SLOW_SEGMENT_HANDLED_POSITION));
        assertThat(slowStored.covers(token(SLOW_SEGMENT_UNHANDLED_POSITION)))
                .as("the stored token of segment %s covers the unhandled event at position %s for instance '%s'",
                    slow, SLOW_SEGMENT_UNHANDLED_POSITION, lostId)
                .isFalse();
        assertThat(storedToken(fast))
                .as("the fast segment keeps its own position; its completion was not diverted elsewhere")
                .isEqualTo(token(FAST_SEGMENT_HANDLED_POSITION));
    }

    /**
     * A checkpoint requested for one segment must not reach another segment's trigger. This is the unit-level form of
     * the oracle above, without the token store: the trigger is the only channel through which a stored token can be
     * advanced, so a request arriving at the wrong trigger is the whole defect.
     */
    @Test
    void aCheckpointRequestReachesOnlyTheTriggerOfItsOwnSegment() {
        var a = FOUR_SEGMENTS.get(0);
        var b = FOUR_SEGMENTS.get(1);
        var triggerA = mock(CheckpointTrigger.class);
        var triggerB = mock(CheckpointTrigger.class);
        checkpointingSupport.onSegmentClaimed(a, null, triggerA);
        checkpointingSupport.onSegmentClaimed(b, null, triggerB);

        checkpointingSupport.requestCheckpoint(a, token(FAST_SEGMENT_HANDLED_POSITION));

        verify(triggerA).requestCheckpoint(token(FAST_SEGMENT_HANDLED_POSITION));
        verifyNoInteractions(triggerB);
    }

    /**
     * Releasing one segment ends that segment's claim only. The trigger it hands back is permanently inert, so it must
     * not be used again, while every segment the node still holds keeps checkpointing.
     */
    @Test
    void releasingOneSegmentKeepsTheOtherSegmentsCheckpointing() {
        var released = FOUR_SEGMENTS.get(0);
        var held = FOUR_SEGMENTS.get(1);
        var releasedTrigger = mock(CheckpointTrigger.class);
        var heldTrigger = mock(CheckpointTrigger.class);
        checkpointingSupport.onSegmentClaimed(released, null, releasedTrigger);
        checkpointingSupport.onSegmentClaimed(held, null, heldTrigger);

        checkpointingSupport.onSegmentReleased(released, token(FAST_SEGMENT_HANDLED_POSITION)).join();

        checkpointingSupport.requestCheckpoint(held, token(SLOW_SEGMENT_HANDLED_POSITION));
        verify(heldTrigger).requestCheckpoint(token(SLOW_SEGMENT_HANDLED_POSITION));

        checkpointingSupport.requestCheckpoint(released, token(SLOW_SEGMENT_UNHANDLED_POSITION));
        verifyNoInteractions(releasedTrigger);
    }

    /**
     * Stand-in for one segment's work package: owns the real framework {@link CheckpointingProgressStrategy} and the
     * segment's stored token, exactly as {@code WorkPackage} does.
     */
    private final class SegmentWorker implements SegmentProgressContext {

        private final Segment segment;
        private final CheckpointingProgressStrategy strategy;
        private final List<Long> handledPositions = new ArrayList<>();
        @Nullable
        private TrackingToken lastConsumed;
        @Nullable
        private TrackingToken stored;

        private SegmentWorker(Segment segment, boolean autoCheckpointing) {
            this.segment = segment;
            this.strategy = new CheckpointingProgressStrategy(this,
                                                              List.<Checkpointing>of(checkpointingSupport),
                                                              autoCheckpointing);
        }

        @Override
        public Segment segment() {
            return segment;
        }

        @Nullable
        @Override
        public TrackingToken lastConsumedToken() {
            return lastConsumed;
        }

        @Override
        public void scheduleWorker() {
            // The test drives the commit cycles explicitly.
        }

        @Override
        public CompletableFuture<Void> persistProgress(@Nullable TrackingToken candidate, ProcessingContext context) {
            // Mirrors WorkPackage#storeIfAdvanced: monotonic, a non-advancing token is ignored.
            if (candidate == null || candidate.equals(stored) || (stored != null && !candidate.covers(stored))) {
                return CompletableFuture.completedFuture(null);
            }
            stored = candidate;
            return tokenStore.storeToken(candidate, PROCESSOR, segment.getSegmentId(), context);
        }

        private void claim() {
            strategy.onSegmentClaimed();
        }

        private void consume(long position) {
            lastConsumed = token(position);
            handledPositions.add(position);
        }

        private void handle(EventMessage eventMessage, long position) {
            consume(position);
            workflowEngine.handle(eventMessage, batchContext(position));
        }

        private void commit() {
            strategy.onBatchCommit(batchContext(null)).join();
        }

        /**
         * A processor batch context carrying this segment, its position and, as the processor does, its trigger.
         */
        private ProcessingContext batchContext(@Nullable Long position) {
            var context = processingContext(segment, position == null ? lastConsumed : token(position));
            strategy.contributeBatchResources(context);
            return context;
        }
    }

    private ProcessingContext processingContext(Segment segment, @Nullable TrackingToken trackingToken) {
        var context = new StubProcessingContext();
        context.putResource(Segment.RESOURCE_KEY, segment);
        if (trackingToken != null) {
            context.putResource(TrackingToken.RESOURCE_KEY, trackingToken);
        }
        return context;
    }

    /**
     * Registers a workflow definition that starts {@link #STRAGGLER_ID} from {@link #START_EVENT} and captures the
     * termination handler the engine installs, so the test can complete the workflow body asynchronously, after the
     * segment that handled its start event has moved on.
     */
    private WorkflowExecution registerStartConfiguration() {
        var state = mock(WorkflowState.class);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.COMPLETED);

        var execution = WorkflowExecutionFixture.mockExecution(STRAGGLER_ID, state, false);
        doAnswer(invocation -> {
            stragglerTermination.set(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        }).when(execution).execute(any());

        var configuration = WorkflowExecutionFixture.mockConfiguration(STRAGGLER_ID, execution);
        when(configuration.workflowIdProvider()).thenReturn(event -> STRAGGLER_ID);
        when(configuration.workflowVersion()).thenReturn("1.0.0");
        when(configurationRegistry.getHighestVersionConfigurations(new MessageType(START_EVENT)))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, configuration)));
        return execution;
    }

    private static EventMessage startEvent() {
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(START_EVENT));
        when(eventMessage.payloadAs(any(TypeReference.class))).thenReturn(Map.of("id", STRAGGLER_ID));
        return eventMessage;
    }

    private static EventMessage engineEvent(String workflowId) {
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.with("workflowId", workflowId));
        when(eventMessage.type()).thenReturn(new MessageType("SomeStepCompleted"));
        return eventMessage;
    }

    private TrackingToken storedToken(Segment segment) {
        return tokenStore.fetchToken(PROCESSOR, segment.getSegmentId(), null).join();
    }

    private static String anotherIdOn(Segment segment, String otherThan) {
        return IntStream.range(1, 512)
                        .mapToObj(i -> "sharded-" + i)
                        .filter(candidate -> !candidate.equals(otherThan))
                        .filter(candidate -> WorkflowSegmentOwnership.ownedBy(segment, candidate))
                        .findFirst()
                        .orElseThrow();
    }
}
