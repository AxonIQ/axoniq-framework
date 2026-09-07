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
package io.axoniq.framework.workflow.runtime.test.fakes;

import io.axoniq.framework.workflow.runtime.execution.DefaultWorkflowScheduler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the {@code WorkflowScheduler} determinism seam: the virtual-time {@link ManualWorkflowScheduler} must fire
 * scheduled deadlines (step timeouts, wait-for-event timeouts, retry backoff) purely by advancing virtual time, and
 * the real-time {@link DefaultWorkflowScheduler} must keep firing on the wall clock.
 */
class ManualWorkflowSchedulerTest {

    @Test
    void manualScheduler_taskFiresOnlyWhenVirtualTimeReachesTheDeadline() {
        var scheduler = new ManualWorkflowScheduler();
        var fired = new AtomicInteger();

        var task = scheduler.schedule(Instant.EPOCH.plusSeconds(5));
        task.completion().thenRun(fired::incrementAndGet);

        scheduler.advanceBy(Duration.ofSeconds(4));
        assertThat(fired).as("before the deadline, the task must not run").hasValue(0);
        assertThat(task.completion()).isNotDone();

        scheduler.advanceBy(Duration.ofSeconds(1));
        assertThat(fired).hasValue(1);
        assertThat(task.completion()).isCompleted();
    }

    @Test
    void manualScheduler_cancelledTaskNeverFires() {
        var scheduler = new ManualWorkflowScheduler();
        var fired = new AtomicInteger();

        var task = scheduler.schedule(Instant.EPOCH.plusSeconds(5));
        task.completion().thenRun(fired::incrementAndGet);
        task.cancel();
        scheduler.advanceBy(Duration.ofSeconds(10));

        assertThat(fired).hasValue(0);
        assertThat(task.completion()).isCancelled();
        assertThat(scheduler.pendingTasks()).isZero();
    }

    @Test
    void manualScheduler_sameAdvancementIsReproducible() {
        // Two schedulers advanced identically must fire identically - the seam contract DST relies on.
        for (int run = 0; run < 2; run++) {
            var scheduler = new ManualWorkflowScheduler();
            var first = new AtomicInteger();
            var second = new AtomicInteger();
            scheduler.schedule(Instant.EPOCH.plusMillis(100)).completion().thenRun(first::incrementAndGet);
            scheduler.schedule(Instant.EPOCH.plusMillis(200)).completion().thenRun(second::incrementAndGet);
            scheduler.advanceBy(Duration.ofMillis(150));
            assertThat(first).hasValue(1);
            assertThat(second).hasValue(0);
            assertThat(scheduler.pendingTasks()).isOne();
        }
    }

    @Test
    void defaultScheduler_firesOnTheWallClock() throws Exception {
        var clock = java.time.Clock.systemUTC();
        var scheduler = new DefaultWorkflowScheduler(clock, new java.util.concurrent.ScheduledThreadPoolExecutor(1));
        var fired = new AtomicInteger();

        var task = scheduler.schedule(clock.instant().plusMillis(20));
        task.completion().thenRun(fired::incrementAndGet);

        task.completion().get(2, TimeUnit.SECONDS);
        assertThat(fired).hasValue(1);
    }
}
