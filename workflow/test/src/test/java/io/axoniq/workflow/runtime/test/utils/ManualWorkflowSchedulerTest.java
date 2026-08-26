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
package io.axoniq.workflow.runtime.test.utils;

import io.axoniq.workflow.runtime.execution.WorkflowScheduler;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ManualWorkflowScheduler}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class ManualWorkflowSchedulerTest {

    @Test
    void runDueTasksRunsOnlyTasksAtOrBeforeNow() {
        ManualWorkflowScheduler scheduler = new ManualWorkflowScheduler();
        Instant now = Instant.parse("2026-06-19T10:15:30Z");
        AtomicInteger invocations = new AtomicInteger();

        WorkflowScheduler.ScheduledTask first = scheduler.schedule(now.plusSeconds(5));
        WorkflowScheduler.ScheduledTask second = scheduler.schedule(now.plusSeconds(10));
        first.completion().thenRun(invocations::incrementAndGet);
        second.completion().thenRun(invocations::incrementAndGet);

        scheduler.runDueTasks(now.plusSeconds(5));

        assertThat(invocations).hasValue(1);
        assertThat(first.completion()).isCompleted();
        assertThat(second.completion()).isNotDone();

        scheduler.runDueTasks(now.plusSeconds(10));

        assertThat(invocations).hasValue(2);
        assertThat(second.completion()).isCompleted();
    }

    @Test
    void cancelledTaskDoesNotRun() {
        ManualWorkflowScheduler scheduler = new ManualWorkflowScheduler();
        Instant now = Instant.parse("2026-06-19T10:15:30Z");
        AtomicInteger invocations = new AtomicInteger();

        WorkflowScheduler.ScheduledTask task = scheduler.schedule(now);
        task.completion().thenRun(invocations::incrementAndGet);
        task.cancel();
        scheduler.runDueTasks(now);

        assertThat(invocations).hasValue(0);
        assertThat(task.completion()).isCancelled();
    }

    @Test
    void deadlineCompletionRunsDependentAction() {
        ManualWorkflowScheduler scheduler = new ManualWorkflowScheduler();
        Instant now = Instant.parse("2026-06-19T10:15:30Z");
        AtomicInteger invocations = new AtomicInteger();

        WorkflowScheduler.ScheduledTask task = scheduler.schedule(now);
        task.completion().thenRun(invocations::incrementAndGet);
        scheduler.runDueTasks(now);

        assertThat(task.completion()).isCompleted();
        assertThat(invocations).hasValue(1);
    }
}
