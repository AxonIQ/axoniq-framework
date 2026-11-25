/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.token;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Tests of the {@link SegmentClaims} and the {@link SegmentClaim claims} they hold.
 *
 * @author Allard Buijze
 */
class SegmentClaimsTest {

    private static final Segment SEGMENT = Segment.ROOT_SEGMENT;
    private static final TrackingToken STARTUP_HEAD = token(10);

    // The phases in which a claim activated, in the order the activations ran.
    private final List<String> activations = new CopyOnWriteArrayList<>();
    // The catch-up polls that are due, which run only when the test runs them.
    private final Queue<Runnable> duePolls = new ConcurrentLinkedQueue<>();
    private final AtomicReference<@Nullable TrackingToken> segmentPosition = new AtomicReference<>();
    private final SegmentClaims testSubject = new SegmentClaims((segment, phase) -> activations.add(phase));

    @BeforeEach
    void setUp() {
        testSubject.catchUpPolls = duePolls::add;
        testSubject.setSegmentPositions(segment -> Optional.ofNullable(segmentPosition.get()));
    }

    private SegmentClaim claimBehindTheStartupHead() {
        testSubject.initialize(STARTUP_HEAD);
        return testSubject.startClaim(SEGMENT, new StubProcessingContext());
    }

    @Nested
    class Activation {

        @Test
        void activatesOnceThePositionCoversTheStartupHead() {
            // given
            var claim = claimBehindTheStartupHead();

            // when
            boolean behind = claim.activate(token(5), () -> activations.add("at 5"));
            boolean caughtUp = claim.activate(token(10), () -> activations.add("at 10"));
            boolean again = claim.activate(token(11), () -> activations.add("at 11"));

            // then
            assertThat(behind).isFalse();
            assertThat(caughtUp).isTrue();
            assertThat(again).isTrue();
            assertThat(activations).containsExactly("at 10");
            assertThat(claim.isWaiting()).isFalse();
        }

        @Test
        void activatesRightAwayWithoutAStartupHead() {
            // given
            testSubject.initialize(null);
            var claim = testSubject.startClaim(SEGMENT, new StubProcessingContext());

            // when
            boolean activated = claim.activate(token(5), () -> activations.add("at 5"));

            // then
            assertThat(activated).isTrue();
            assertThat(activations).containsExactly("at 5");
        }

        @Test
        void neverActivatesAFailedClaim() {
            // given
            var claim = claimBehindTheStartupHead();
            claim.fail(() -> {
            });

            // when
            boolean activated = claim.activate(token(10), () -> activations.add("at 10"));

            // then - not waiting to catch up, but not activated either
            assertThat(activated).isTrue();
            assertThat(activations).isEmpty();
            assertThat(claim.isWaiting()).isFalse();
        }

        @Test
        void neverActivatesAnEndedClaim() {
            // given
            var claim = claimBehindTheStartupHead();
            testSubject.release(SEGMENT);

            // when
            boolean activated = claim.activate(token(10), () -> activations.add("at 10"));

            // then
            assertThat(activated).isTrue();
            assertThat(activations).isEmpty();
            assertThat(claim.isWaiting()).isFalse();
        }

        @Test
        void activatesAClaimResumingAtTheStartupHeadOnClaim() {
            // given
            var claim = claimBehindTheStartupHead();

            // when
            testSubject.activateOrAwaitCatchUp(claim, STARTUP_HEAD);

            // then
            assertThat(activations).containsExactly("on claim of segment " + SEGMENT.getSegmentId());
            assertThat(duePolls).isEmpty();
        }

        @Test
        void activatesAClaimBehindTheStartupHeadOnceThePolledPositionCoversIt() {
            // given
            var claim = claimBehindTheStartupHead();
            testSubject.activateOrAwaitCatchUp(claim, token(5));
            segmentPosition.set(token(7));
            runDuePoll();
            assertThat(activations).isEmpty();

            // when
            segmentPosition.set(STARTUP_HEAD);
            runDuePoll();

            // then - activated once, and polling ended
            assertThat(activations).containsExactly("after catch-up");
            assertThat(claim.isWaiting()).isFalse();
            runDuePoll();
            assertThat(duePolls).isEmpty();
        }

        @Test
        void pollsNoPositionOnceStopped() {
            // given
            var claim = claimBehindTheStartupHead();
            testSubject.activateOrAwaitCatchUp(claim, token(5));
            segmentPosition.set(STARTUP_HEAD);

            // when
            testSubject.stop();
            runDuePoll();

            // then
            assertThat(activations).isEmpty();
            assertThat(duePolls).isEmpty();
            assertThat(claim.isWaiting()).isTrue();
        }

        @Test
        void keepsPollingWhenReadingThePositionFails() {
            // given
            var claim = claimBehindTheStartupHead();
            testSubject.setSegmentPositions(segment -> {
                throw new IllegalStateException("The position cannot be read.");
            });
            testSubject.activateOrAwaitCatchUp(claim, token(5));

            // when
            runDuePoll();

            // then - not activated, and the next poll activates once the position can be read
            assertThat(activations).isEmpty();
            assertThat(claim.isWaiting()).isTrue();
            testSubject.setSegmentPositions(segment -> Optional.of(STARTUP_HEAD));
            runDuePoll();
            assertThat(activations).containsExactly("after catch-up");
        }

        @Test
        void stopsPollingOnceTheSegmentIsReleased() {
            // given
            var claim = claimBehindTheStartupHead();
            testSubject.activateOrAwaitCatchUp(claim, token(5));
            segmentPosition.set(STARTUP_HEAD);

            // when
            testSubject.release(SEGMENT);
            runDuePoll();

            // then
            assertThat(activations).isEmpty();
            assertThat(duePolls).isEmpty();
        }

        @Test
        void activatesNothingOnDeliveryWithoutASegmentOrAToken() {
            // given
            var claim = claimBehindTheStartupHead();

            // when
            testSubject.activateOnDelivery(null, STARTUP_HEAD);
            testSubject.activateOnDelivery(SEGMENT, null);

            // then
            assertThat(activations).isEmpty();
            assertThat(claim.isWaiting()).isTrue();
        }

        @Test
        void activatesAnImplicitClaimOnDeliveryOfAnEventAtTheStartupHead() {
            // given - a segment handling events without having been claimed through the engine
            testSubject.initialize(STARTUP_HEAD);

            // when
            testSubject.activateOnDelivery(SEGMENT, token(9));
            var implicitClaim = testSubject.claimOf(SEGMENT).orElseThrow();
            boolean waitingBefore = implicitClaim.isWaiting();
            testSubject.activateOnDelivery(SEGMENT, STARTUP_HEAD);

            // then
            assertThat(implicitClaim.executionContext()).isNull();
            assertThat(waitingBefore).isTrue();
            assertThat(activations).containsExactly("after catch-up");
            assertThat(testSubject.claimOf(SEGMENT)).containsSame(implicitClaim);
        }

        private void runDuePoll() {
            requireNonNull(duePolls.poll(), "No catch-up poll is due.").run();
        }
    }

    @Nested
    class StartWhileCurrent {

        @Test
        void letsBodiesStartBeforeTheActivationWithoutAStartupHead() {
            // given
            testSubject.initialize(null);
            var claim = testSubject.startClaim(SEGMENT, new StubProcessingContext());
            var mayStart = new ArrayList<Boolean>();

            // when
            boolean ran = claim.startWhileCurrent(mayStart::add);

            // then - and the activation still runs, once
            assertThat(ran).isTrue();
            assertThat(mayStart).containsExactly(true);
            assertThat(claim.activate(null, () -> activations.add("activated"))).isTrue();
            assertThat(claim.activate(null, () -> activations.add("activated"))).isTrue();
            assertThat(activations).containsExactly("activated");
        }

        @Test
        void letsBodiesStartOnlyOnceTheClaimActivated() {
            // given
            var claim = claimBehindTheStartupHead();
            var mayStart = new ArrayList<Boolean>();

            // when
            claim.startWhileCurrent(mayStart::add);
            claim.activate(STARTUP_HEAD, () -> {
            });
            claim.startWhileCurrent(mayStart::add);

            // then
            assertThat(mayStart).containsExactly(false, true);
        }

        @Test
        void runsNothingOnceTheClaimFailed() {
            // given
            var failed = claimBehindTheStartupHead();
            failed.fail(() -> {
            });

            // when
            boolean started = failed.startWhileCurrent(mayStart -> fail("Started on a failed claim."));
            boolean ran = failed.runWhileCurrent(() -> fail("Ran on a failed claim."));

            // then
            assertThat(started).isFalse();
            assertThat(ran).isFalse();
        }

        @Test
        void runsNothingOnceTheClaimEnded() {
            // given
            var ended = claimBehindTheStartupHead();
            testSubject.release(SEGMENT);

            // when
            boolean started = ended.startWhileCurrent(mayStart -> fail("Started on an ended claim."));
            boolean ran = ended.runWhileCurrent(() -> fail("Ran on an ended claim."));

            // then
            assertThat(started).isFalse();
            assertThat(ran).isFalse();
        }
    }

    @Nested
    class StartExclusively {

        @Test
        void letsBodiesStartOutsideASegmentedProcessor() {
            // given
            testSubject.initialize(STARTUP_HEAD);
            var mayStart = new ArrayList<Boolean>();

            // when
            testSubject.startExclusively(null, mayStart::add);

            // then
            assertThat(mayStart).containsExactly(true);
        }

        @Test
        void recordsAnImplicitClaimForASegmentWithoutOne() {
            // given
            testSubject.initialize(STARTUP_HEAD);
            var mayStart = new ArrayList<Boolean>();

            // when
            testSubject.startExclusively(SEGMENT, mayStart::add);

            // then
            assertThat(mayStart).containsExactly(false);
            assertThat(testSubject.claimOf(SEGMENT)).hasValueSatisfying(
                    claim -> assertThat(claim.executionContext()).isNull()
            );
        }

        @Test
        void runsForAClaimThatEndedButLetsNoBodiesStart() {
            // given
            var claim = claimBehindTheStartupHead();
            claim.activate(STARTUP_HEAD, () -> {
            });
            testSubject.release(SEGMENT);
            var mayStart = new ArrayList<Boolean>();

            // when
            claim.startExclusively(mayStart::add);

            // then
            assertThat(mayStart).containsExactly(false);
        }
    }

    @Nested
    class Failure {

        @Test
        void runsNoCleanupForAClaimThatEnded() {
            // given - a claim that a later claim of its segment replaced
            var replaced = claimBehindTheStartupHead();
            var current = testSubject.startClaim(SEGMENT, new StubProcessingContext());
            var cleanups = new ArrayList<String>();

            // when
            replaced.fail(() -> cleanups.add("replaced"));

            // then - and the current claim is unaffected
            assertThat(cleanups).isEmpty();
            assertThat(current.runWhileCurrent(() -> cleanups.add("current"))).isTrue();
            assertThat(testSubject.hasFailed(SEGMENT)).isFalse();
        }

        @Test
        void runsTheCleanupOfTheCurrentClaimAndHoldsItsCheckpoint() {
            // given
            var claim = claimBehindTheStartupHead();
            var cleanups = new ArrayList<String>();

            // when
            claim.fail(() -> cleanups.add("current"));

            // then
            assertThat(cleanups).containsExactly("current");
            assertThat(testSubject.hasFailed(SEGMENT)).isTrue();
            assertThat(testSubject.holdsCheckpoint(SEGMENT)).isTrue();
        }

        @Test
        void runsTheCleanupOnlyOnceWhenAFailedClaimThatEndedFailsAgain() {
            // given - a failed claim that ended, as a next claim of its segment started
            var claim = claimBehindTheStartupHead();
            var cleanups = new ArrayList<String>();
            claim.fail(() -> cleanups.add("first"));
            testSubject.startClaim(SEGMENT, new StubProcessingContext());

            // when
            claim.fail(() -> cleanups.add("second"));

            // then - the second cleanup would drop what the next claim restores
            assertThat(cleanups).containsExactly("first");
            assertThat(claim.hasFailed()).isTrue();
            assertThat(testSubject.hasFailed(SEGMENT)).isFalse();
        }

        @Test
        void keepsAFailedClaimFailedWhenItEnds() {
            // given
            var claim = claimBehindTheStartupHead();
            claim.fail(() -> {
            });

            // when
            testSubject.release(SEGMENT);

            // then - a thread that still holds the claim keeps seeing it failed
            assertThat(claim.hasFailed()).isTrue();
            assertThat(claim.holdsCheckpoint()).isTrue();
            assertThat(testSubject.hasFailed(SEGMENT)).isFalse();
        }
    }

    @Nested
    class CheckpointHold {

        @Test
        void holdsTheCheckpointOfTheCurrentClaimUntilItEnds() {
            // given
            claimBehindTheStartupHead();

            // when
            testSubject.holdCheckpoint(SEGMENT);

            // then
            assertThat(testSubject.holdsCheckpoint(SEGMENT)).isTrue();
            assertThat(testSubject.hasFailed(SEGMENT)).isFalse();
            testSubject.startClaim(SEGMENT, new StubProcessingContext());
            assertThat(testSubject.holdsCheckpoint(SEGMENT)).isFalse();
        }

        @Test
        void holdsTheCheckpointOfAnImplicitClaimForASegmentWithoutOne() {
            // when
            testSubject.holdCheckpoint(SEGMENT);

            // then
            assertThat(testSubject.holdsCheckpoint(SEGMENT)).isTrue();
        }
    }

    @Nested
    class End {

        @Test
        void waitsForAnActionHoldingTheLockOfTheClaim() throws InterruptedException {
            // given - an action of the claim that runs until the test releases it
            var claim = claimBehindTheStartupHead();
            var actionRuns = new CountDownLatch(1);
            var actionMayEnd = new CountDownLatch(1);
            var action = Thread.ofVirtual().start(() -> claim.runWhileCurrent(() -> {
                actionRuns.countDown();
                try {
                    actionMayEnd.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
            assertThat(actionRuns.await(5, TimeUnit.SECONDS)).isTrue();

            try {
                // when
                var release = Thread.ofVirtual().start(() -> testSubject.release(SEGMENT));

                // then - the release waits for the action, after which the claim runs no action anymore
                await().atMost(5, TimeUnit.SECONDS).until(() -> claim.isWaitingForLock(release));
                actionMayEnd.countDown();
                assertThat(release.join(Duration.ofSeconds(5))).isTrue();
                assertThat(action.join(Duration.ofSeconds(5))).isTrue();
                assertThat(claim.runWhileCurrent(() -> fail("Ran on an ended claim."))).isFalse();
            } finally {
                actionMayEnd.countDown();
            }
        }
    }

    @Nested
    class RunWhileCurrent {

        @Test
        void letsAnActionWaitForAVirtualThreadWhileAVirtualThreadPerCarrierRunsAnActionOfTheClaim()
                throws InterruptedException {
            // given: as many virtual threads as there are carrier threads, each running an action of the same claim
            // that waits for a virtual thread, the way hibernating workflow threads wait for the lock of the timers
            var claim = testSubject.startClaim(SEGMENT, new StubProcessingContext());
            var released = new CountDownLatch(1);
            var actions = new ArrayList<Thread>();
            for (int i = 0; i < carrierThreadCount(); i++) {
                actions.add(Thread.ofVirtual().start(() -> claim.runWhileCurrent(() -> {
                    try {
                        released.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                })));
            }
            try {
                // every action but the one holding the lock of the claim waits for that lock
                await().atMost(5, TimeUnit.SECONDS).until(
                        () -> actions.stream().filter(claim::isWaitingForLock).count() == actions.size() - 1
                );

                // when
                var releaser = Thread.ofVirtual().start(released::countDown);

                // then: the virtual thread they wait for runs, and so does every action
                assertThat(releaser.join(Duration.ofSeconds(5))).isTrue();
                for (var action : actions) {
                    assertThat(action.join(Duration.ofSeconds(5))).isTrue();
                }
            } finally {
                // Frees the carrier threads, also when the virtual thread above never ran.
                released.countDown();
            }
        }
    }

    private static int carrierThreadCount() {
        return Integer.getInteger("jdk.virtualThreadScheduler.parallelism", Runtime.getRuntime().availableProcessors());
    }
}
