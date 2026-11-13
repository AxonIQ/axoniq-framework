/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Wall-clock timeout scheduler used in production.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class DefaultWorkflowScheduler implements WorkflowScheduler {

    private final Clock clock;
    private final ScheduledThreadPoolExecutor timerExecutor;

    /**
     * Creates a scheduler backed by the provided timer executor.
     *
     * @param clock         workflow clock
     * @param timerExecutor executor used only for deadline delivery
     */
    public DefaultWorkflowScheduler(Clock clock, ScheduledThreadPoolExecutor timerExecutor) {
        this.clock = Objects.requireNonNull(clock, "Clock must not be null");
        this.timerExecutor = Objects.requireNonNull(timerExecutor, "Timer executor must not be null");
        this.timerExecutor.setRemoveOnCancelPolicy(true);
        this.timerExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    @Override
    public ScheduledTask schedule(Instant deadline) {
        var completion = new CompletableFuture<Void>();
        var delay = Duration.between(clock.instant(), deadline);
        var delayMillis = Math.max(0, delay.toMillis());
        ScheduledFuture<?> scheduledTask = timerExecutor.schedule(
                () -> {
                    if (completion.isDone()) {
                        return;
                    }
                    completion.complete(null);
                },
                delayMillis,
                TimeUnit.MILLISECONDS
        );
        return new ScheduledTask() {
            @Override
            public CompletableFuture<Void> completion() {
                return completion;
            }

            @Override
            public void cancel() {
                if (scheduledTask.cancel(false)) {
                    completion.cancel(false);
                }
            }
        };
    }

    /**
     * Stops deadline delivery and cancels pending timer tasks.
     */
    public void shutdown() {
        timerExecutor.shutdownNow();
    }
}
