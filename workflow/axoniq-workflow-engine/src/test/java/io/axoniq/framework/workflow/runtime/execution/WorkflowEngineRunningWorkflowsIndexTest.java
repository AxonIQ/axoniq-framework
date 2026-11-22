/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.axoniq.framework.workflow.runtime.execution.LifecycleEventFixtures.*;
import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.token;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies how a {@link WorkflowEngine} keeps its index of running workflows per segment: every checkpoint and release
 * writes the pending changes first and fails when it cannot, and a claim restores the stored index plus the lifecycle
 * events from the token it resumes from.
 *
 * @author Allard Buijze
 */
class WorkflowEngineRunningWorkflowsIndexTest {

    private static final String INDEX = "orders";
    private static final Segment SEGMENT = Segment.ROOT_SEGMENT;

    private final ControllableWorkflowIndex workflowIndex = new ControllableWorkflowIndex();
    private final RecordingWorkflowStore workflowStore = new RecordingWorkflowStore();
    private final InMemoryEventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final EventStore eventStore = eventStore(storageEngine);
    private final List<TrackingToken> requestedCheckpoints = new CopyOnWriteArrayList<>();
    private WorkflowEngine engine;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;

    /**
     * Returns a real unit of work factory: the engine sources every restored instance in a unit of work of its own.
     */
    private static SimpleUnitOfWorkFactory unitOfWorkFactory() {
        return new SimpleUnitOfWorkFactory(new ApplicationContext() {
            @Override
            public <C> C component(Class<C> type, String name) {
                throw new ComponentNotFoundException(type, name);
            }
        });
    }

    private static <T> T await(CompletableFuture<T> future) {
        return future.orTimeout(5, TimeUnit.SECONDS).join();
    }

    @BeforeEach
    void setUp() {
        createEngine(workflowIndex);
    }

    private void createEngine(WorkflowIndex index) {
        engine = new WorkflowEngine(new SimpleWorkflowConfigurationRegistry(),
                                    new InMemoryWorkflowExecutionRepository(),
                                    new WorkflowCancellationService(),
                                    workflowStore,
                                    index,
                                    INDEX,
                                    unitOfWorkFactory());
        checkpointingSupport = new WorkflowEngineCheckpointingSupport(engine, 0L);
        engine.setCheckpointingSupport(checkpointingSupport);
    }

    private void claim(@Nullable TrackingToken claimedFrom) {
        checkpointingSupport.onSegmentClaimed(SEGMENT, claimedFrom, requestedCheckpoints::add);
        ProcessingContext claimContext = StubProcessingContext.withComponent(EventStore.class, eventStore);
        await(engine.restoreWorkflowsFor(SEGMENT, claimedFrom, claimContext, new StubProcessingContext()));
    }

    private void handle(EventMessage event, long position) {
        engine.handle(event, handlingContext(SEGMENT, token(position)));
    }

    /**
     * Mirrors how the processor releases a segment: its checkpointing support reports the released position, after
     * which the segment change listener releases the workflows of the segment.
     */
    private CompletableFuture<TrackingToken> release(TrackingToken upTo) {
        var released = checkpointingSupport.onSegmentReleased(SEGMENT, upTo);
        await(engine.releaseWorkflowsFor(SEGMENT));
        return released;
    }

    private Set<String> stored() {
        return await(workflowIndex.load(INDEX, SEGMENT)).orElseThrow()
                                                        .stream()
                                                        .map(WorkflowIndex.Entry::workflowId)
                                                        .collect(Collectors.toSet());
    }

    private CompletableFuture<TrackingToken> checkpoint(long position) {
        return checkpointingSupport.onCheckpointAdvanced(SEGMENT, token(position));
    }

    @Nested
    class Checkpoints {

        @BeforeEach
        void setUp() {
            workflowIndex.initialized(INDEX);
            claim(token(0));
        }

        @Test
        void areRequestedAfterEveryEventWithoutWritingTheIndex() {
            // when
            handle(stepEvent("order-1"), 1);
            handle(lifecycleEvent("order-1", WorkflowStatus.STARTED), 2);

            // then
            assertThat(requestedCheckpoints).containsExactly(token(1), token(2));
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }

        @Test
        void completeOnlyOnceEveryChangeHandledBeforeThemIsDurable() {
            // given
            handle(lifecycleEvent("order-1", WorkflowStatus.STARTED), 1);
            handle(lifecycleEvent("order-2", WorkflowStatus.STARTED), 2);

            // when
            var checkpoint = checkpoint(2);

            // then
            assertThat(checkpoint).isNotDone();
            workflowIndex.completeNextWrite();
            assertThat(await(checkpoint)).isEqualTo(token(2));
            assertThat(stored()).containsExactlyInAnyOrder("order-1", "order-2");
        }

        @Test
        void completeRightAwayOnAQuietSegment() {
            // given
            handle(stepEvent("order-1"), 1);
            handle(stepEvent("order-1"), 2);

            // when
            var checkpoint = checkpoint(2);

            // then
            assertThat(checkpoint).isCompletedWithValue(token(2));
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }

        @Test
        void failWhenTheIndexCannotBeWritten() {
            // given
            workflowIndex.failWritesImmediately(new IllegalStateException("database down"));
            handle(lifecycleEvent("order-1", WorkflowStatus.STARTED), 1);

            // when
            var checkpoint = checkpoint(1);

            // then - the processor stores nothing, and releases the segment
            assertThat(checkpoint).failsWithin(Duration.ofSeconds(5));
        }
    }

    @Nested
    class WithoutAnIndex {

        private static final String LONG_ID = "w".repeat(1000);

        @BeforeEach
        void setUp() {
            createEngine(WorkflowIndex.none());
            claim(token(0));
        }

        @Test
        void neverWaitsForAnIndexOnACheckpoint() {
            // when
            handle(lifecycleEvent(LONG_ID, WorkflowStatus.STARTED), 1);
            handle(lifecycleEvent("order-1", WorkflowStatus.STARTED), 2);

            // then
            assertThat(requestedCheckpoints).containsExactly(token(1), token(2));
            assertThat(checkpoint(2)).isCompletedWithValue(token(2));
        }

        @Test
        void releasesAtTheConsumedToken() {
            // given
            handle(lifecycleEvent(LONG_ID, WorkflowStatus.STARTED), 1);

            // when
            var released = release(token(3));

            // then
            assertThat(await(released)).isEqualTo(token(3));
        }

        @Test
        void restoresTheRunningWorkflowListOnEveryClaim() {
            // given
            await(release(token(1)));

            // when
            claim(token(1));

            // then
            assertThat(workflowStore.runningWorkflowLoads()).isEqualTo(2);
        }
    }

    @Nested
    class FailedEvent {

        private final List<Segment> releasedSegments = new CopyOnWriteArrayList<>();

        @BeforeEach
        void setUp() {
            workflowIndex.initialized(INDEX);
            engine.setSegmentReleaser(releasedSegments::add);
            claim(token(0));
        }

        @Test
        void holdsTheCheckpointAndAsksForAReleaseWhenTheIndexCannotFollowTheEvent() {
            // given - a lifecycle event whose status the index of running workflows cannot read
            handle(stepEvent("order-1"), 1);
            var unreadable = EventTestUtils.asEventMessage("lifecycle")
                                           .andMetadata(MetadataUtils.create("order-1")
                                                                        .and(MetadataUtils.METADATA_KEY_WORKFLOW_STATUS,
                                                                             "UNKNOWN"));

            // when
            assertThatThrownBy(() -> handle(unreadable, 2)).isInstanceOf(IllegalArgumentException.class);
            // and the processor's error handler swallows the failure and hands the segment the next event
            handle(stepEvent("order-1"), 3);

            // then - no checkpoint and no release stores a position past the failed event, and the processor is asked
            // to release the segment, so its next claim handles that event again
            assertThat(requestedCheckpoints).containsExactly(token(1));
            assertThat(releasedSegments).containsExactly(SEGMENT);
            assertThat(release(token(3))).failsWithin(Duration.ofSeconds(5));
        }
    }

    @Nested
    class Release {

        @BeforeEach
        void setUp() {
            workflowIndex.initialized(INDEX);
        }

        @Test
        void writesThePendingChangesBeforeReportingTheReleasedToken() {
            // given
            claim(token(0));
            handle(lifecycleEvent("order-1", WorkflowStatus.STARTED), 1);
            handle(lifecycleEvent("order-2", WorkflowStatus.STARTED), 2);

            // when
            var released = checkpointingSupport.onSegmentReleased(SEGMENT, token(4));

            // then - the processor gets the released token once the index holds every change
            assertThat(released).isNotDone();
            workflowIndex.completeNextWrite();
            assertThat(await(released)).isEqualTo(token(4));
            assertThat(stored()).containsExactlyInAnyOrder("order-1", "order-2");
            await(engine.releaseWorkflowsFor(SEGMENT));
        }

        @Test
        void failsWhenThePendingChangesCannotBeWritten() {
            // given
            claim(token(4));
            handle(stepEvent("order-1"), 5);
            handle(lifecycleEvent("order-1", WorkflowStatus.STARTED), 6);
            workflowIndex.failWritesImmediately(new IllegalStateException("database down"));

            // when
            var released = release(token(7));

            // then - the release does not advance the stored token
            assertThat(released).failsWithin(Duration.ofSeconds(5));
        }
    }

    @Nested
    class Claim {

        @Test
        void restoresTheRunningWorkflowListAndInitializesTheIndexWhenItIsNotInitialized() {
            // given
            workflowStore.running("order-1", "order-2");

            // when
            claim(token(0));

            // then
            assertThat(workflowStore.runningWorkflowLoads()).isEqualTo(1);
            assertThat(workflowStore.sourcedWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-2");
            assertThat(stored()).containsExactlyInAnyOrder("order-1", "order-2");
        }

        @Test
        void restoresTheStoredIndexPlusTheWorkflowsStartedAfterTheClaimedToken() {
            // given - a first claim indexed order-1 and order-2, and released the segment
            workflowIndex.completeWritesImmediately();
            append(storageEngine,
                   lifecycleEvent("order-1", WorkflowStatus.STARTED),
                   lifecycleEvent("order-2", WorkflowStatus.STARTED));
            claim(null);
            handle(lifecycleEvent("order-1", WorkflowStatus.STARTED), 1);
            handle(lifecycleEvent("order-2", WorkflowStatus.STARTED), 2);
            assertThat(await(release(token(2)))).isEqualTo(token(2));
            // and - meanwhile another node started order-3 and completed order-2
            append(storageEngine,
                   lifecycleEvent("order-3", WorkflowStatus.STARTED),
                   lifecycleEvent("order-2", WorkflowStatus.COMPLETED));
            var loadsBeforeReclaim = workflowStore.runningWorkflowLoads();

            // when
            claim(token(2));

            // then
            assertThat(workflowStore.runningWorkflowLoads()).isEqualTo(loadsBeforeReclaim);
            assertThat(workflowStore.sourcedWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-3");
        }

        @Test
        void removesIndexEntriesOfWorkflowsWhoseStateIsTerminal() {
            // given - the index still lists order-1, though it completed: a write adding it landed after its removal
            workflowIndex.initialized(INDEX, "order-1", "order-2").completeWritesImmediately();
            workflowStore.withState("order-1", VersionedType.of("OrderWorkflow", "1.0"), WorkflowStatus.COMPLETED);

            // when
            claim(token(1));
            await(checkpoint(1));

            // then
            assertThat(stored()).containsExactly("order-2");
        }

        @Test
        void restoresTheRunningWorkflowListWhenTheIndexCannotBeLoaded() {
            // given
            workflowIndex.failLoads(new IllegalStateException("database down"));
            workflowStore.running("order-1", "order-2");

            // when
            claim(token(0));

            // then
            assertThat(workflowStore.sourcedWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-2");
        }

        @Test
        void restoresFromTheIndexAndTheLifecycleEventsFromTheClaimedToken() {
            // given - an index of order-1, written before the processor stored the token after its start
            workflowIndex.initialized(INDEX, "order-1");
            append(storageEngine, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            // and - events after that token
            append(storageEngine,
                   lifecycleEvent("order-2", WorkflowStatus.STARTED),
                   lifecycleEvent("order-1", WorkflowStatus.COMPLETED),
                   stepEvent("order-2"));

            // when
            claim(token(1));

            // then
            assertThat(workflowStore.sourcedWorkflowIds()).containsExactly("order-2");
            assertThat(workflowStore.runningWorkflowLoads()).isZero();
        }

        @Test
        void restoresEveryRunningWorkflowAfterACheckpointFailedToWriteTheIndex() {
            // given - a previous claim handled starts and ends without its index ever being written
            workflowIndex.initialized(INDEX).failWritesImmediately(new IllegalStateException("database down"));
            append(storageEngine,
                   lifecycleEvent("order-1", WorkflowStatus.STARTED),
                   lifecycleEvent("order-2", WorkflowStatus.STARTED),
                   lifecycleEvent("order-1", WorkflowStatus.COMPLETED));
            claim(token(0));
            handle(lifecycleEvent("order-1", WorkflowStatus.STARTED), 1);
            handle(lifecycleEvent("order-2", WorkflowStatus.STARTED), 2);
            handle(lifecycleEvent("order-1", WorkflowStatus.COMPLETED), 3);
            // and - its checkpoint failed, so the processor kept the claimed token and released the segment
            assertThat(checkpoint(3)).failsWithin(Duration.ofSeconds(5));
            assertThat(release(token(3))).failsWithin(Duration.ofSeconds(5));
            workflowIndex.completeWritesImmediately();

            // when - the segment is claimed again from the stored token
            claim(token(0));

            // then - the index plus the lifecycle events from that token restore the running workflow
            assertThat(workflowStore.sourcedWorkflowIds()).containsExactly("order-2", "order-2");
            assertThat(workflowStore.runningWorkflowLoads()).isZero();
        }

        @Test
        void writesTheChangesFoundAfterTheClaimedTokenWithTheFirstCheckpoint() {
            // given
            workflowIndex.initialized(INDEX, "order-1").completeWritesImmediately();
            append(storageEngine, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            append(storageEngine,
                   lifecycleEvent("order-2", WorkflowStatus.STARTED),
                   lifecycleEvent("order-1", WorkflowStatus.COMPLETED));

            claim(token(1));

            // when
            await(checkpoint(1));

            // then
            assertThat(stored()).containsExactly("order-2");
        }
    }
}
