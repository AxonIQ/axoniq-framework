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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Publisher that delays the publication of events.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class DelayedPublisher {

    private static final Logger logger = LoggerFactory.getLogger(DelayedPublisher.class);

    private final Executor executor;
    private final List<Schedule> schedules = new ArrayList<>();
    private final TestEventPublisher eventPublisher;

    public DelayedPublisher(TestEventPublisher eventPublisher,
                            Executor executor) {
        this.executor = executor;
        this.eventPublisher = eventPublisher;
    }

    public void addSchedules(List<Schedule> schedules) {
        this.schedules.addAll(schedules);
    }

    public CompletableFuture<Void> start() {
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

        for (Schedule schedule : schedules) {
            future = future
                    .thenCompose(v ->
                                         CompletableFuture
                                                 .supplyAsync(() -> {
                                                                  this.eventPublisher.publish(schedule.event);
                                                                  return null;
                                                              },
                                                              CompletableFuture.delayedExecutor(schedule.duration.toMillis(),
                                                                                                TimeUnit.MILLISECONDS,
                                                                                                executor))
                    );
        }
        return future;
    }

    public record Schedule(
            Duration duration,
            Object event
    ) {

        public static Schedule of(Duration duration, Object event) {
            return new Schedule(duration, event);
        }

        public static Schedule ofMillis(long millis, Object event) {
            return new Schedule(Duration.ofMillis(millis), event);
        }
    }
}
