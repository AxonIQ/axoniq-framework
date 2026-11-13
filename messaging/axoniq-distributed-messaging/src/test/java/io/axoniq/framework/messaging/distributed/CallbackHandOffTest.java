/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.distributed;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Test class validating {@link CallbackHandOff}.
 */
class CallbackHandOffTest {

    @Test
    void shouldRunTheTaskOnAnotherThread() {
        // given
        AtomicReference<Thread> runner = new AtomicReference<>();
        Runnable callback = CallbackHandOff.handOff(() -> runner.set(Thread.currentThread()));

        // when
        callback.run();

        // then
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(runner.get()).isNotNull());
        assertThat(runner.get()).isNotSameAs(Thread.currentThread());
        assertThat(runner.get().isVirtual()).isTrue();
    }

    @Test
    void shouldNotRunTheTaskOnTheThreadThatSignals() {
        // given
        Queue<Runnable> scheduled = new ArrayDeque<>();
        AtomicInteger runs = new AtomicInteger();
        Runnable callback = CallbackHandOff.handOff(scheduled::add, runs::incrementAndGet);

        // when
        callback.run();

        // then
        assertThat(runs).hasValue(0);
        assertThat(scheduled).hasSize(1);
    }

    @Test
    void shouldShareOneRunBetweenSignalsThatArriveBeforeItStarts() {
        // given
        Queue<Runnable> scheduled = new ArrayDeque<>();
        AtomicInteger runs = new AtomicInteger();
        Runnable callback = CallbackHandOff.handOff(scheduled::add, runs::incrementAndGet);

        // when
        callback.run();
        callback.run();
        callback.run();

        // then
        assertThat(scheduled).hasSize(1);

        scheduled.remove().run();

        assertThat(runs).hasValue(1);
    }

    @Test
    void shouldScheduleTheTaskAgainWhenASignalArrivesWhileItRuns() {
        // given
        Queue<Runnable> scheduled = new ArrayDeque<>();
        AtomicReference<Runnable> callback = new AtomicReference<>();
        AtomicInteger runs = new AtomicInteger();

        callback.set(CallbackHandOff.handOff(scheduled::add, () -> {
            if (runs.incrementAndGet() == 1) {
                callback.get().run();  // a signal that arrives after the task looked, while it is still running
            }
        }));

        // when
        callback.get().run();
        scheduled.remove().run();

        // then
        assertThat(scheduled).hasSize(1);

        scheduled.remove().run();

        assertThat(runs).hasValue(2);
    }

    @Test
    void shouldScheduleTheTaskAgainAfterItRan() {
        // given
        Queue<Runnable> scheduled = new ArrayDeque<>();
        AtomicInteger runs = new AtomicInteger();
        Runnable callback = CallbackHandOff.handOff(scheduled::add, runs::incrementAndGet);

        callback.run();
        scheduled.remove().run();

        // when
        callback.run();

        // then
        assertThat(scheduled).hasSize(1);
    }

    @Test
    void shouldThrowWhenTheExecutorRejectsTheTaskAndAcceptLaterSignals() {
        // given
        Queue<Runnable> scheduled = new ArrayDeque<>();
        AtomicInteger attempts = new AtomicInteger();
        Runnable callback = CallbackHandOff.handOff(task -> {
            if (attempts.incrementAndGet() == 1) {
                throw new RejectedExecutionException("no capacity");
            }

            scheduled.add(task);
        }, () -> { });

        // when / then
        assertThatThrownBy(callback::run).isInstanceOf(RejectedExecutionException.class);

        callback.run();

        assertThat(scheduled).hasSize(1);
    }

    @Test
    void shouldRejectANullExecutorOrTask() {
        assertThatThrownBy(() -> CallbackHandOff.handOff(null, () -> { })).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> CallbackHandOff.handOff(Runnable::run, null)).isInstanceOf(NullPointerException.class);
    }
}
