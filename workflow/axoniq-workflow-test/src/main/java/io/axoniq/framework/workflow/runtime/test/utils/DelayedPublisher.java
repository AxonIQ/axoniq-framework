/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.utils;

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
 * @since 5.4.0
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
