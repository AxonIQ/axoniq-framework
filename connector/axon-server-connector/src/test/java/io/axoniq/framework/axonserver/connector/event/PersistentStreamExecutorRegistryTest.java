/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.event;

import org.junit.jupiter.api.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link PersistentStreamExecutorRegistry}.
 *
 * @author Steven van Beelen
 */
class PersistentStreamExecutorRegistryTest {

    private final PersistentStreamExecutorRegistry testSubject = new PersistentStreamExecutorRegistry();

    @Nested
    class Track {

        @Test
        void returnsTheTrackedExecutor() {
            // given
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

            // when
            ScheduledExecutorService result = testSubject.track(executor);

            // then
            assertThat(result).isSameAs(executor);
            executor.shutdownNow();
        }

        @SuppressWarnings({"DataFlowIssue", "resource"})
        @Test
        void rejectsNullExecutor() {
            // when / then
            assertThatThrownBy(() -> testSubject.track(null)).isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class ShutdownAll {

        @Test
        void shutsDownEveryTrackedExecutor() {
            // given
            ScheduledExecutorService first = testSubject.track(Executors.newSingleThreadScheduledExecutor());
            ScheduledExecutorService second = testSubject.track(Executors.newSingleThreadScheduledExecutor());

            // when
            testSubject.shutdownAll();

            // then
            assertThat(first.isShutdown()).isTrue();
            assertThat(second.isShutdown()).isTrue();
        }

        @Test
        void leavesUntrackedExecutorsRunning() {
            // given
            try (ScheduledExecutorService untracked = Executors.newSingleThreadScheduledExecutor()) {

                // when
                testSubject.shutdownAll();

                // then
                assertThat(untracked.isShutdown()).isFalse();
                untracked.shutdownNow();
            }
        }

        @Test
        void doesNothingWhenNothingIsTracked() {
            // when / then
            testSubject.shutdownAll();
        }
    }
}
