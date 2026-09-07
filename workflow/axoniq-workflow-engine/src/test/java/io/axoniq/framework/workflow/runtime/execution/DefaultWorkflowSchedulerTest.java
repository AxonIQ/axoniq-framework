/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. You may not use this file except in compliance
 * with the License.
 *
 * You may obtain a copy of the License at:
 * https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 * https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests for {@link DefaultWorkflowScheduler}.
 *
 * @author Simon Zambrovski
 */
class DefaultWorkflowSchedulerTest {

    @Test
    void scheduledDeadlinesCompleteIndependently() {
        var timerExecutor = new ScheduledThreadPoolExecutor(4);
        var scheduler = new DefaultWorkflowScheduler(Clock.systemUTC(), timerExecutor);
        try {
            var now = Instant.now();
            var first = scheduler.schedule(now);
            var second = scheduler.schedule(now);

            await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> {
                assertThat(first.completion()).isCompleted();
                assertThat(second.completion()).isCompleted();
            });
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void cancelRemovesThePendingTimerAndPreventsItsTaskFromRunning() {
        var timerExecutor = new ScheduledThreadPoolExecutor(1);
        try {
            var now = Instant.parse("2026-08-19T12:00:00Z");
            var scheduler = new DefaultWorkflowScheduler(Clock.fixed(now, ZoneOffset.UTC), timerExecutor);
            var scheduledTask = scheduler.schedule(now.plusSeconds(10));
            assertThat(timerExecutor.getQueue()).hasSize(1);

            scheduledTask.cancel();

            await().atMost(Duration.ofSeconds(1)).untilAsserted(() ->
                    assertThat(timerExecutor.getQueue()).isEmpty()
            );
            assertThat(scheduledTask.completion()).isCancelled();
        } finally {
            timerExecutor.shutdownNow();
        }
    }

    @Test
    void shutdownCancelsPendingTimersAndPreventsTheirTasksFromRunning() {
        var timerExecutor = new ScheduledThreadPoolExecutor(1);
        var scheduler = new DefaultWorkflowScheduler(Clock.systemUTC(), timerExecutor);
        scheduler.schedule(Instant.now().plusSeconds(10));
        scheduler.shutdown();

        assertThat(timerExecutor.isShutdown()).isTrue();
        assertThat(timerExecutor.getQueue()).isEmpty();
    }
}
