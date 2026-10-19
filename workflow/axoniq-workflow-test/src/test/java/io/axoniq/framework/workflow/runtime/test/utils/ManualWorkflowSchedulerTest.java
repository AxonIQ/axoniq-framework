/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.utils;

import io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ManualWorkflowScheduler}.
 *
 * @author Simon Zambrovski
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
