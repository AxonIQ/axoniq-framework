/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.api.WorkflowIndex.Entry;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.IndexDelta;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.OpenWait;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflowsClaimLoader.ClaimedIndex;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.token;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests how the {@link WorkflowEngineHibernationSupport} adopts the hibernated workflows whose hints a claim accepted:
 * when they wake, and what they record in the {@link RunningWorkflowsProjection}. Which hibernated workflows a
 * broadcast event addresses is up to the {@link LiveWorkflows}, see {@code LiveWorkflowsTest}.
 * <p>
 * Every collaborator is a real one, except the loader, which records the wakes and brings a stand-in execution into
 * memory in place of the hibernation: loading and starting a workflow is the engine's part, see
 * {@link WorkflowEngineHibernationSupport.WorkflowLoader}.
 *
 * @author Allard Buijze
 */
class WorkflowEngineHibernationSupportAdoptionTest {

    private static final String INDEX = "orders";
    private static final Segment SEGMENT = Segment.ROOT_SEGMENT;

    private final ControllableWorkflowIndex workflowIndex = new ControllableWorkflowIndex();
    private final RunningWorkflowsProjection projection = new RunningWorkflowsProjection(workflowIndex, INDEX);
    // The test activates its claim itself, see activate().
    private final SegmentClaims segmentClaims = new SegmentClaims((segment, phase) -> {
    });
    private final WorkflowCancellationService cancellationService = new WorkflowCancellationService();
    private final AtomicInteger firedTimers = new AtomicInteger();
    private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1) {
        @Override
        protected void afterExecute(Runnable task, @Nullable Throwable failure) {
            // After the timer completed its task, and with it what that task completion runs, like a wake it submits.
            firedTimers.incrementAndGet();
        }
    };
    private final ExecutorService wakeThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicInteger wakesInProgress = new AtomicInteger();
    // Counts each wake from its submission until it ended.
    private final Executor wakeExecutor = task -> {
        wakesInProgress.incrementAndGet();
        wakeThreads.execute(() -> {
            try {
                task.run();
            } finally {
                wakesInProgress.decrementAndGet();
            }
        });
    };
    private final List<String> loads = new CopyOnWriteArrayList<>();
    private final LiveWorkflows liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
    // Records the wake, and brings an execution into memory in place of the hibernation, as the engine does,
    // swapping it back for the hibernation when the action before its start fails.
    private final WorkflowEngineHibernationSupport.WorkflowLoader wakeInMemory = (hibernated, beforeStart) -> {
        loads.add(hibernated.workflowId());
        var execution = new InspectionCountingWorkflowExecution(hibernated.workflowId());
        var woken = new AtomicBoolean();
        hibernated.claim().runWhileCurrent(() -> woken.set(liveWorkflows.wake(hibernated, execution)));
        if (woken.get()) {
            try {
                beforeStart.accept(execution);
            } catch (RuntimeException e) {
                hibernated.claim().runWhileCurrent(() -> liveWorkflows.hibernate(execution, hibernated));
                throw e;
            }
        }
        return woken.get();
    };
    private volatile WorkflowEngineHibernationSupport.WorkflowLoader loader = wakeInMemory;
    private final WorkflowEngineHibernationSupport support = new WorkflowEngineHibernationSupport(
            new DefaultWorkflowScheduler(Clock.systemUTC(), timer),
            Clock.systemUTC(),
            liveWorkflows,
            cancellationService,
            projection,
            (hibernated, beforeStart) -> loader.load(hibernated, beforeStart),
            Duration.ofSeconds(30),
            wakeExecutor
    );
    private SegmentClaim claim;

    @BeforeEach
    void setUp() {
        workflowIndex.initialized(INDEX).completeWritesImmediately();
        // A claim that has the startup backlog to catch up with.
        segmentClaims.initialize(token(10));
        claim = segmentClaims.startClaim(SEGMENT, new StubProcessingContext());
        support.start();
    }

    @AfterEach
    void tearDown() {
        support.stop();
        timer.shutdownNow();
        wakeThreads.shutdownNow();
    }

    private static HibernationHint hint(Instant wakeAt, String orderId) {
        return new HibernationHint(wakeAt, List.of(
                new OpenWait("awaitPaid", "paid", Set.of("payload:orderId=" + orderId))
        ));
    }

    private static HibernationHint pastDeadline(String orderId) {
        return hint(Instant.now().minus(Duration.ofMinutes(1)), orderId);
    }

    private static HibernationHint futureDeadline(String orderId) {
        return hint(Instant.now().plus(Duration.ofHours(1)), orderId);
    }

    /**
     * Tracks the segment as a claim that accepted the given hints does, and adopts them.
     */
    private void adopt(Map<String, HibernationHint> hints) {
        hints.forEach((workflowId, hint) -> workflowIndex.stored(INDEX, new Entry(workflowId, hint)));
        projection.initialize(SEGMENT, new ClaimedIndex(Set.of(), hints, Map.of()));
        hints.forEach((workflowId, hint) -> assertThat(support.adopt(claim, workflowId, hint)).isTrue());
    }

    private void activate() {
        assertThat(claim.activate(token(10), () -> support.arm(SEGMENT))).isTrue();
    }

    private static EventMessage paid(String orderId) {
        return new GenericEventMessage(new MessageType("paid"), Map.of("orderId", orderId));
    }

    /**
     * Returns whether every wake submitted so far ended, so a wake that may load a workflow has had its chance to.
     */
    private boolean noWakeInProgress() {
        return wakesInProgress.get() == 0;
    }

    @Nested
    class Deadline {

        @Test
        void wakesAnAdoptedWorkflowWhoseDeadlinePassedOnlyOnceItsClaimCaughtUp() {
            // given
            adopt(Map.of("wf-1", pastDeadline("1")));

            // when - the claim still catches up, as the deadline passes
            await().atMost(5, TimeUnit.SECONDS).until(() -> firedTimers.get() >= 1 && noWakeInProgress());
            assertThat(loads).isEmpty();
            activate();

            // then
            await().atMost(5, TimeUnit.SECONDS).until(() -> liveWorkflows.find("wf-1").isPresent());
            assertThat(loads).containsExactly("wf-1");
            assertThat(liveWorkflows.hibernatedWorkflowIds()).isEmpty();
        }

        @Test
        void wakesAWorkflowAdoptedAfterItsClaimCaughtUpRightAway() {
            // given
            projection.initialize(SEGMENT, new ClaimedIndex(Set.of(), Map.of(), Map.of()));
            activate();

            // when
            assertThat(support.adopt(claim, "wf-1", pastDeadline("1"))).isTrue();

            // then
            await().atMost(5, TimeUnit.SECONDS).until(() -> liveWorkflows.find("wf-1").isPresent());
            assertThat(loads).containsExactly("wf-1");
        }

        @Test
        void loadsAtMostABoundedNumberOfWorkflowsAtOnceWhenManyDeadlinesPassed() {
            // given - wakes that load until the test lets them finish
            var running = new AtomicInteger();
            var mostAtOnce = new AtomicInteger();
            var finish = new CountDownLatch(1);
            loader = (hibernated, beforeStart) -> {
                mostAtOnce.accumulateAndGet(running.incrementAndGet(), Math::max);
                try {
                    finish.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                running.decrementAndGet();
                return wakeInMemory.load(hibernated, beforeStart);
            };
            var hints = new HashMap<String, HibernationHint>();
            IntStream.range(0, 3 * WorkflowEngineHibernationSupport.MAX_CONCURRENT_DETACHED_WAKES)
                     .forEach(i -> hints.put("wf-" + i, pastDeadline(String.valueOf(i))));
            adopt(hints);

            // when
            activate();

            // then - once every wake either loads a workflow or waits for a permit to
            await().atMost(5, TimeUnit.SECONDS)
                   .until(() -> running.get() + support.wakesWaitingForAPermit() == hints.size());
            assertThat(running).hasValue(WorkflowEngineHibernationSupport.MAX_CONCURRENT_DETACHED_WAKES);
            finish.countDown();
            await().atMost(5, TimeUnit.SECONDS).until(() -> liveWorkflows.hibernatedWorkflowIds().isEmpty());
            assertThat(loads).hasSize(hints.size());
            assertThat(mostAtOnce).hasValue(WorkflowEngineHibernationSupport.MAX_CONCURRENT_DETACHED_WAKES);
        }
    }

    @Nested
    class Event {

        @Test
        void clearsTheHintOfAnAdoptedWorkflowThatWoke() {
            // given
            adopt(Map.of("wf-1", futureDeadline("1")));

            // when
            liveWorkflows.broadcast(SEGMENT, paid("1"), new StubProcessingContext());
            projection.decideCheckpoint(SEGMENT).write(null).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(workflowIndex.issuedWrites()).singleElement().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of("wf-1"), Set.of()))
            );
            assertThat(workflowIndex.durableEntries(INDEX, SEGMENT)).containsExactly(Entry.running("wf-1"));
        }

        @Test
        void keepsTheHintOfAnAdoptedWorkflowWhoseWakeFailed() {
            // given
            adopt(Map.of("wf-1", futureDeadline("1")));
            loader = (hibernated, beforeStart) -> {
                throw new IllegalStateException("event store down");
            };

            // when
            try {
                liveWorkflows.broadcast(SEGMENT, paid("1"), new StubProcessingContext());
            } catch (IllegalStateException expected) {
                // fails the event, so the processor delivers it again
            }
            projection.decideCheckpoint(SEGMENT).write(null).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(liveWorkflows.hibernatedWorkflowIds()).containsExactly("wf-1");
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }
    }

    @Nested
    class Cancellation {

        @Test
        void wakesAnAdoptedWorkflowForACancellationOnlyOnceItsClaimCaughtUp() {
            // given
            adopt(Map.of("wf-1", futureDeadline("1")));

            // when
            cancellationService.requestWorkflowCancellation("wf-1", null);

            // then
            await().atMost(5, TimeUnit.SECONDS)
                   .until(WorkflowEngineHibernationSupportAdoptionTest.this::noWakeInProgress);
            assertThat(loads).isEmpty();
            activate();
            await().atMost(5, TimeUnit.SECONDS).until(() -> loads.equals(List.of("wf-1")) && noWakeInProgress());
            // the wake succeeded, and queued the request on the woken execution, which runs it once its body started
            assertThat(liveWorkflows.hibernatedWorkflowIds()).isEmpty();
            assertThat(liveWorkflows.find("wf-1")).hasValueSatisfying(
                    woken -> assertThat(((InspectionCountingWorkflowExecution) woken).appendedTasks()).hasSize(1)
            );
        }
    }

    @Nested
    class Adoption {

        @Test
        void listsAnAdoptedWorkflowAsHibernatedWithoutWakingIt() {
            // when
            adopt(Map.of("wf-1", futureDeadline("1")));

            // then
            assertThat(liveWorkflows.hibernatedWorkflowIds()).containsExactly("wf-1");
            assertThat(liveWorkflows.isHibernated("wf-1")).isTrue();
            assertThat(liveWorkflows.find("wf-1")).isEmpty();
            assertThat(loads).isEmpty();
        }

        @Test
        void adoptsNoWorkflowThatIsLiveAlready() {
            // given
            adopt(Map.of("wf-1", futureDeadline("1")));
            var hibernated = liveWorkflows.get("wf-1");

            // when
            var adopted = support.adopt(claim, "wf-1", futureDeadline("2"));

            // then
            assertThat(adopted).isFalse();
            assertThat(liveWorkflows.get("wf-1")).isSameAs(hibernated);
        }

        @Test
        void adoptsNothingOnceTheClaimEnded() {
            // given
            segmentClaims.release(SEGMENT);

            // when
            var adopted = support.adopt(claim, "wf-1", futureDeadline("1"));

            // then
            assertThat(adopted).isFalse();
            assertThat(liveWorkflows.hibernatedWorkflowIds()).isEmpty();
        }

        @Test
        void dropsOnlyTheAdoptedWorkflowsOfTheGivenClaim() {
            // given - a workflow adopted by a claim whose restore later failed, and one of the claim after it
            adopt(Map.of("wf-1", futureDeadline("1")));
            var later = segmentClaims.startClaim(SEGMENT, new StubProcessingContext());
            assertThat(support.adopt(later, "wf-2", futureDeadline("2"))).isTrue();

            // when
            support.drop(claim);

            // then
            assertThat(liveWorkflows.hibernatedWorkflowIds()).containsExactly("wf-2");
        }
    }
}
