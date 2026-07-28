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
package io.axoniq.workflow.runtime.execution;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Wall-clock timeout scheduler used in production.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class DefaultWorkflowScheduler implements WorkflowScheduler {

    private final Clock clock;

    /**
     * Creates a scheduler.
     *
     * @param clock workflow clock.
     */
    public DefaultWorkflowScheduler(@Nonnull Clock clock) {
        this.clock = Objects.requireNonNull(clock, "Clock must not be null");
    }

    @Nonnull
    @Override
    public ScheduledTask schedule(@Nonnull Instant deadline, @Nonnull Runnable task) {
        var completion = new CompletableFuture<Void>();
        var delay = Duration.between(clock.instant(), deadline);
        var delayMillis = Math.max(0, delay.toMillis());
        CompletableFuture.runAsync(
                () -> {
                    if (completion.isDone()) {
                        return;
                    }
                    try {
                        task.run();
                        completion.complete(null);
                    } catch (Throwable t) {
                        completion.completeExceptionally(t);
                    }
                },
                CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS)
        );
        return new ScheduledTask() {
            @Nonnull
            @Override
            public CompletableFuture<Void> completion() {
                return completion;
            }

            @Override
            public void cancel() {
                completion.cancel(false);
            }
        };
    }
}
