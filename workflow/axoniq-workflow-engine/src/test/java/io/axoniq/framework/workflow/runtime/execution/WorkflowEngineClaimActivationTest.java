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
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry.PredicatedWorkflowConfiguration;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkConfiguration;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.axoniq.framework.workflow.runtime.execution.LifecycleEventFixtures.*;
import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.token;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Verifies when a {@link WorkflowEngine} starts the workflow executions of a claimed segment: once per claim, never
 * twice for the same execution, never held back by an earlier claim of the segment, and never for a claim whose restore
 * failed, which leaves the segment inert until the processor released it.
 *
 * @author Allard Buijze
 */
class WorkflowEngineClaimActivationTest {

    private static final Segment SEGMENT = Segment.ROOT_SEGMENT;
    private static final String WORKFLOW_ID = "order-1";
    private static final VersionedType DEFINITION_ID = VersionedType.of(new QualifiedName("OrderWorkflow"), "1.0.0");
    private static final QualifiedName START_EVENT = new QualifiedName("OrderPlaced");
    private static final TrackingToken STARTUP_HEAD = token(10);
    // More polls than a claim waiting to catch up runs before it stops polling a segment the processor does not list.
    private static final int MAX_CATCH_UP_POLLS = 100;

    private final RecordingWorkflowStore workflowStore = new RecordingWorkflowStore();
    private final WorkflowConfigurationRegistry<?> configurationRegistry = mock(WorkflowConfigurationRegistry.class);
    private final List<String> bodyStarts = new CopyOnWriteArrayList<>();
    private final List<Segment> releasedSegments = new CopyOnWriteArrayList<>();
    private final List<TrackingToken> requestedCheckpoints = new CopyOnWriteArrayList<>();
    private final AtomicReference<@Nullable TrackingToken> segmentPosition = new AtomicReference<>();
    private final AtomicInteger segmentPositionReads = new AtomicInteger();
    private final AtomicInteger unitsOfWorkInProgress = new AtomicInteger();
    // The catch-up polls the engine scheduled, which run only when the test runs them.
    private final Queue<Runnable> duePolls = new ConcurrentLinkedQueue<>();
    private final EventStore eventStore = eventStore(new InMemoryEventStorageEngine());
    private WorkflowEngine engine;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;

    @BeforeEach
    void setUp() {
        engine = new WorkflowEngine(configurationRegistry,
                                    new InMemoryWorkflowExecutionRepository(),
                                    new WorkflowCancellationService(),
                                    workflowStore,
                                    // Without an index, every claim restores the running workflow list of the store.
                                    WorkflowIndex.none(),
                                    "orders",
                                    new SimpleUnitOfWorkFactory(new ApplicationContext() {
                                        @Override
                                        public <C> C component(Class<C> type, String name) {
                                            throw new ComponentNotFoundException(type, name);
                                        }
                                    }, this::countUnitsOfWorkInProgress));
        engine.catchUpPolls = duePolls::add;
        checkpointingSupport = new WorkflowEngineCheckpointingSupport(engine, 0L);
        engine.setCheckpointingSupport(checkpointingSupport);
        engine.setSegmentPositions(segment -> {
            segmentPositionReads.incrementAndGet();
            return Optional.ofNullable(segmentPosition.get());
        });
        engine.setSegmentReleaser(releasedSegments::add);
        engine.start(STARTUP_HEAD).orTimeout(5, TimeUnit.SECONDS).join();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown();
    }

    /**
     * Counts the units of work of the engine, such as the one restoring an instance, until they ended.
     */
    private UnitOfWorkConfiguration countUnitsOfWorkInProgress(UnitOfWorkConfiguration configuration) {
        return configuration.registerProcessingLifecycleEnhancer(lifecycle -> {
            unitsOfWorkInProgress.incrementAndGet();
            lifecycle.doFinally(context -> unitsOfWorkInProgress.decrementAndGet());
        });
    }

    /**
     * Runs the catch-up poll that is due first, as the poll interval passing does.
     */
    private void runCatchUpPoll() {
        requireNonNull(duePolls.poll(), "No catch-up poll is due.").run();
    }

    /**
     * Runs the catch-up polls that are due, and the polls they schedule in turn, until no poll is due anymore.
     *
     * @return the number of polls that ran
     */
    private int runCatchUpPolls() {
        int polls = 0;
        for (var poll = duePolls.poll(); poll != null; poll = duePolls.poll()) {
            assertThat(++polls).as("catch-up polls that keep on polling").isLessThanOrEqualTo(MAX_CATCH_UP_POLLS);
            poll.run();
        }
        return polls;
    }

    private CompletableFuture<Void> claim(TrackingToken claimedFrom) {
        checkpointingSupport.onSegmentClaimed(SEGMENT, claimedFrom, requestedCheckpoints::add);
        return engine.restoreWorkflowsFor(SEGMENT,
                                          claimedFrom,
                                          StubProcessingContext.withComponent(EventStore.class, eventStore),
                                          new StubProcessingContext())
                     .orTimeout(5, TimeUnit.SECONDS);
    }

    /**
     * Makes {@link #WORKFLOW_ID} a workflow whose executions record their body starts, and returns the configuration
     * creating them.
     */
    private WorkflowCancellationProvider recordingExecution() {
        var execution = WorkflowExecutionFixture.mockExecution(
                WORKFLOW_ID, new EventSourcedWorkflowState(WORKFLOW_ID, DEFINITION_ID), false
        );
        WorkflowExecutionFixture.recordBodyStartOn(execution, bodyStarts::add, WORKFLOW_ID);
        var configuration = WorkflowExecutionFixture.mockConfiguration(WORKFLOW_ID, execution);
        when(configuration.workflowIdProvider()).thenReturn(event -> WORKFLOW_ID);
        when(configuration.workflowVersion()).thenReturn(DEFINITION_ID.version());
        when(configurationRegistry.getWorkflowConfiguration(DEFINITION_ID)).thenReturn(Optional.of(configuration));
        when(configurationRegistry.getHighestVersionConfigurations(new MessageType(START_EVENT)))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((event, context) -> true, configuration)));
        return (WorkflowCancellationProvider) execution;
    }

    private static EventMessage startEvent() {
        return EventTestUtils.asEventMessage(
                new GenericMessage(new MessageType(START_EVENT), Map.of("orderId", WORKFLOW_ID))
        );
    }

    @Nested
    class FailedRestore {

        @BeforeEach
        void setUp() {
            workflowStore.failRunningWorkflowLoads(new IllegalStateException("event store down"));
        }

        @Test
        void asksTheProcessorToReleaseTheSegment() {
            // when
            var claimed = claim(STARTUP_HEAD);

            // then
            assertThatThrownBy(claimed::join).hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(releasedSegments).containsExactly(SEGMENT);
        }

        @Test
        void failsEveryEventOfTheSegmentSoTheProcessorReleasesItWithoutCheckpointing() {
            // given
            recordingExecution();
            assertThatThrownBy(claim(STARTUP_HEAD)::join).hasRootCauseInstanceOf(IllegalStateException.class);

            // when - the segment handles an event before the processor released it
            var handled = engine.handle(startEvent(), handlingContext(SEGMENT, token(11)));

            // then - the batch fails, so the processor stores nothing for it and releases the segment
            assertThat(handled.error()).isPresent();
            assertThat(engine.workflowExecutions()).isEmpty();
            assertThat(bodyStarts).isEmpty();
        }

        @Test
        void neitherRequestsACheckpointNorReportsAPositionOnRelease() {
            // given
            assertThatThrownBy(claim(STARTUP_HEAD)::join).hasRootCauseInstanceOf(IllegalStateException.class);

            // when
            checkpointingSupport.requestCheckpoint(SEGMENT, token(11));
            var released = checkpointingSupport.onSegmentReleased(SEGMENT, token(11));

            // then - the stored token stays where it was, so the next claim handles every event after it again
            assertThat(requestedCheckpoints).isEmpty();
            assertThat(released).isCompletedExceptionally();
        }

        @Test
        void reportsNoPositionOnARepeatedReleaseAfterTheSegmentWasReleased() {
            // given - a failed claim, released by the processor
            assertThatThrownBy(claim(STARTUP_HEAD)::join).hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(checkpointingSupport.onSegmentReleased(SEGMENT, token(11))).isCompletedExceptionally();
            engine.releaseWorkflowsFor(SEGMENT).orTimeout(5, TimeUnit.SECONDS).join();

            // when - the processor reports the release once more, as it does when a segment it was asked to release
            // also failed a batch
            var releasedAgain = checkpointingSupport.onSegmentReleased(SEGMENT, token(11));

            // then
            assertThat(releasedAgain).isCompletedExceptionally();
        }
    }

    @Nested
    class TimedOutRestore {

        private static final String OTHER_WORKFLOW_ID = "order-2";

        @BeforeEach
        void setUp() {
            engine.restoreTimeout = Duration.ofMillis(300);
        }

        @Test
        void doesNotStartTheWorkflowsItRestoredBeforeTimingOut() {
            // given - one instance restores right away, the other one does not before the restore times out
            recordingExecution();
            workflowStore.running(WORKFLOW_ID, OTHER_WORKFLOW_ID)
                         .withState(WORKFLOW_ID, DEFINITION_ID, WorkflowStatus.STARTED)
                         .withState(OTHER_WORKFLOW_ID, DEFINITION_ID, WorkflowStatus.STARTED);
            workflowStore.holdSourcing(OTHER_WORKFLOW_ID);

            // when - the claim is at the startup head, so a successful restore would start its workflows right away
            assertThatThrownBy(claim(STARTUP_HEAD)::join).hasRootCauseInstanceOf(TimeoutException.class);
            segmentPosition.set(STARTUP_HEAD);

            // then - also once every catch-up poll that falls due has run
            runCatchUpPolls();
            assertThat(bodyStarts).isEmpty();
            assertThat(releasedSegments).containsExactly(SEGMENT);
        }

        @Test
        void dropsTheExecutionsItRestoredBeforeTimingOut() {
            // given - one instance restores right away, the other one does not before the restore times out
            recordingExecution();
            workflowStore.running(WORKFLOW_ID, OTHER_WORKFLOW_ID)
                         .withState(WORKFLOW_ID, DEFINITION_ID, WorkflowStatus.STARTED)
                         .withState(OTHER_WORKFLOW_ID, DEFINITION_ID, WorkflowStatus.STARTED);
            workflowStore.holdSourcing(OTHER_WORKFLOW_ID);

            // when
            assertThatThrownBy(claim(STARTUP_HEAD)::join).hasRootCauseInstanceOf(TimeoutException.class);

            // then - nothing of the failed claim stays on this node, also when it is never released here
            assertThat(engine.workflowExecutions()).isEmpty();
        }

        @Test
        void addsNoExecutionForAnInstanceThatFinishesRestoringAfterwards() {
            // given
            recordingExecution();
            workflowStore.running(WORKFLOW_ID).withState(WORKFLOW_ID, DEFINITION_ID, WorkflowStatus.STARTED);
            var sourcing = workflowStore.holdSourcing(WORKFLOW_ID);
            assertThatThrownBy(claim(STARTUP_HEAD)::join).hasRootCauseInstanceOf(TimeoutException.class);

            // when
            sourcing.complete(null);

            // then - once the restore of the instance ended, it added nothing
            await().atMost(Duration.ofSeconds(5)).until(() -> unitsOfWorkInProgress.get() == 0);
            assertThat(engine.workflowExecutions()).isEmpty();
        }

        @Test
        void leavesNoExecutionBehindForAnInstanceThatFinishesRestoringAfterTheRelease() {
            // given - a restore that timed out, and the release the engine asked the processor for
            recordingExecution();
            workflowStore.running(WORKFLOW_ID).withState(WORKFLOW_ID, DEFINITION_ID, WorkflowStatus.STARTED);
            var sourcing = workflowStore.holdSourcing(WORKFLOW_ID);
            assertThatThrownBy(claim(STARTUP_HEAD)::join).hasRootCauseInstanceOf(TimeoutException.class);
            engine.releaseWorkflowsFor(SEGMENT).orTimeout(5, TimeUnit.SECONDS).join();

            // when - the instance finishes restoring after the release
            sourcing.complete(null);

            // then - once the restore of the instance ended, it added nothing: a stale execution would be kept by the
            // next claim of the segment on this node, and run from the state it sourced back then
            await().atMost(Duration.ofSeconds(5)).until(() -> unitsOfWorkInProgress.get() == 0);
            assertThat(engine.workflowExecutions()).isEmpty();
        }
    }

    @Nested
    class ReleaseWithUnfinishedWork {

        @Test
        void reportsNoPositionWhenTheProcessorReportsTheReleaseAgainAfterTheExecutionsWereDropped() {
            // given - a restored execution that stopped with work that is not durable yet
            var execution = recordingExecution();
            doAnswer(invocation -> {
                invocation.<WorkflowExecution.CheckpointWorkStateListener>getArgument(0).onMarkedUnsafe();
                return null;
            }).when((WorkflowExecution) execution).registerCheckpointWorkStateListener(any());
            when(((WorkflowExecution) execution).hasUnsafeCheckpointWork()).thenReturn(true);
            workflowStore.running(WORKFLOW_ID).withState(WORKFLOW_ID, DEFINITION_ID, WorkflowStatus.STARTED);
            claim(STARTUP_HEAD).join();
            // and - the release, which holds the stored token behind that work, after which the engine drops it
            assertThat(checkpointingSupport.onSegmentReleased(SEGMENT, token(11))).isCompletedExceptionally();
            engine.releaseWorkflowsFor(SEGMENT).orTimeout(5, TimeUnit.SECONDS).join();

            // when - the processor reports the release once more, as it does when a segment it was asked to release
            // also failed a batch
            var releasedAgain = checkpointingSupport.onSegmentReleased(SEGMENT, token(11));

            // then - the work was never made durable, so the stored token must not move past it
            assertThat(releasedAgain).isCompletedExceptionally();
        }

        @Test
        void reportsThePositionOfAReleaseWithoutUnfinishedWork() {
            // given
            claim(STARTUP_HEAD).join();

            // when
            var released = checkpointingSupport.onSegmentReleased(SEGMENT, token(11));
            engine.releaseWorkflowsFor(SEGMENT).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(released).isCompletedWithValue(token(11));
        }
    }

    @Nested
    class CatchUpPolling {

        @Test
        void stopsOnceTheProcessorNoLongerListsTheSegment() {
            // given - a claim behind the startup head, of a segment the processor does not list, as when it was
            // claimed elsewhere without a release here
            claim(token(5)).join();

            // when - every catch-up poll runs as it falls due
            int polls = runCatchUpPolls();

            // then - reading its position stops after a few polls, instead of continuing until shutdown
            assertThat(polls).isEqualTo(WorkflowEngine.CATCH_UP_POLLS_WITHOUT_POSITION);
            assertThat(segmentPositionReads).hasValue(WorkflowEngine.CATCH_UP_POLLS_WITHOUT_POSITION);
        }

        @Test
        void stillStartsTheRestoredWorkflowsWhenTheSegmentIsListedAfterAShortAbsence() {
            // given - a claim behind the startup head whose segment the processor does not list for two polls
            recordingExecution();
            workflowStore.running(WORKFLOW_ID).withState(WORKFLOW_ID, DEFINITION_ID, WorkflowStatus.STARTED);
            claim(token(5)).join();
            runCatchUpPoll();
            runCatchUpPoll();

            // when
            segmentPosition.set(STARTUP_HEAD);
            runCatchUpPolls();

            // then
            assertThat(bodyStarts).containsExactly(WORKFLOW_ID);
        }
    }

    @Nested
    class SuccessiveClaims {

        @Test
        void startTheRestoredWorkflowsOfAClaimFollowingACaughtUpClaimThatEndedWithoutARelease() {
            // given - a first claim at the startup head caught up right away, and ended without a release
            claim(STARTUP_HEAD).join();
            // and - the next claim, behind the startup head, restores a running workflow
            recordingExecution();
            workflowStore.running(WORKFLOW_ID)
                         .withState(WORKFLOW_ID, DEFINITION_ID, WorkflowStatus.STARTED);
            claim(token(5)).join();
            assertThat(bodyStarts).isEmpty();

            // when
            segmentPosition.set(STARTUP_HEAD);
            runCatchUpPolls();

            // then
            assertThat(bodyStarts).containsExactly(WORKFLOW_ID);
        }
    }

    @Nested
    class RestoredWorkflowEnd {

        @Test
        void advancesTheCheckpointThroughItsTerminalEventOnly() {
            // given - a running workflow restored by a claim that resumed from a position far behind the segment
            var execution = (WorkflowExecution) recordingExecution();
            var claimedFrom = token(5);
            var termination = new AtomicReference<@Nullable Consumer<WorkflowExecution>>();
            doAnswer(invocation -> {
                termination.set(invocation.getArgument(0));
                return CompletableFuture.completedFuture(null);
            }).when(execution).execute(any());
            workflowStore.running(WORKFLOW_ID).withState(WORKFLOW_ID, DEFINITION_ID, WorkflowStatus.STARTED);
            claim(claimedFrom).join();
            // and - the segment caught up, which starts the restored workflow
            segmentPosition.set(token(50));
            runCatchUpPolls();

            // when - the segment handles the terminal event of the workflow, after which its body ends
            engine.handle(lifecycleEvent(WORKFLOW_ID, WorkflowStatus.COMPLETED), handlingContext(SEGMENT, token(51)));
            requireNonNull(termination.get()).accept(execution);

            // then - the checkpoint advances to the terminal event, never to a position the segment did not handle yet
            assertThat(requestedCheckpoints).containsExactly(token(51));
        }
    }

    @Nested
    class NewInstanceStart {

        @Test
        void startsAnInstanceCreatedAsTheSegmentCatchesUpExactlyOnce() throws InterruptedException {
            // given - a claim behind the startup head, waiting to catch up
            var execution = recordingExecution();
            claim(token(5)).join();
            // and - the handler pauses right after creating the new instance, while the catch-up poll that finds the
            // segment caught up runs on another thread, until that poll has either started the instance or waits for
            // the handler to finish
            var cancellation = execution.workflowCancellation();
            var poller = new Thread(WorkflowEngineClaimActivationTest.this::runCatchUpPolls);
            var createdInstance = new AtomicBoolean();
            when(execution.workflowCancellation()).thenAnswer(invocation -> {
                if (createdInstance.compareAndSet(false, true)) {
                    segmentPosition.set(STARTUP_HEAD);
                    var claim = engine.segmentClaims.claimOf(SEGMENT).orElseThrow();
                    poller.start();
                    await().atMost(Duration.ofSeconds(5)).until(
                            () -> !bodyStarts.isEmpty() || engine.segmentClaims.isWaitingForLock(claim, poller)
                    );
                }
                return cancellation;
            });

            // when - the catch-up poller runs while the handler starts the instance
            engine.handle(startEvent(), handlingContext(SEGMENT, token(6)));

            // then - once that poll and every poll it scheduled ran
            assertThat(createdInstance).isTrue();
            assertThat(poller.join(Duration.ofSeconds(5))).isTrue();
            runCatchUpPolls();
            assertThat(bodyStarts).containsExactly(WORKFLOW_ID);
        }
    }
}
