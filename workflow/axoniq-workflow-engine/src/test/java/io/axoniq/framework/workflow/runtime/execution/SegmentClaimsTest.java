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
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Tests of the {@link SegmentClaims}.
 *
 * @author Allard Buijze
 */
class SegmentClaimsTest {

    private final SegmentClaims testSubject = new SegmentClaims();

    @Nested
    class RunWhileCurrent {

        @Test
        void letsAnActionWaitForAVirtualThreadWhileAVirtualThreadPerCarrierRunsAnActionOfTheClaim()
                throws InterruptedException {
            // given: as many virtual threads as there are carrier threads, each running an action of the same claim
            // that waits for a virtual thread, the way hibernating workflow threads wait for the lock of the timers
            var claim = testSubject.startClaim(Segment.ROOT_SEGMENT, new StubProcessingContext());
            var released = new CountDownLatch(1);
            var actions = new ArrayList<Thread>();
            for (int i = 0; i < carrierThreadCount(); i++) {
                actions.add(Thread.ofVirtual().start(() -> testSubject.runWhileCurrent(claim, () -> {
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
                        () -> actions.stream().filter(action -> testSubject.isWaitingForLock(claim, action)).count()
                                == actions.size() - 1
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
