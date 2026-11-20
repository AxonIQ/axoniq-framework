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

package io.axoniq.framework.kafka.subscribing;

import org.axonframework.common.annotation.Internal;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A request to stop, with the deadline by which stopping should be done.
 * <p>
 * Until the stop is {@link #request(Duration) requested}, waits that use this signal run to their own deadline. Once it
 * is requested, every wait ends at the stop deadline at the latest, also a wait that began before the request, so that
 * all waits while stopping together stay within the time given to stop.
 * <p>
 * Internal, because it only coordinates the shutdown of the components of the subscribing Kafka event source.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
final class StopSignal {

    private final CompletableFuture<Void> requested = new CompletableFuture<>();
    private volatile long deadline;

    /**
     * Requests the stop, with a deadline the given {@code grace} from now. Only the first request sets the deadline;
     * later requests have no effect.
     *
     * @param grace the time from now until the stop deadline
     */
    void request(Duration grace) {
        if (!requested.isDone()) {
            // Written before the request is visible, so that every wait that sees the request sees the deadline.
            deadline = System.nanoTime() + grace.toNanos();
            requested.complete(null);
        }
    }

    /**
     * Returns whether the stop was requested.
     *
     * @return {@code true} once the stop was requested
     */
    private boolean isRequested() {
        return requested.isDone();
    }

    /**
     * Returns the time left until the stop deadline, at most the given {@code cap}. Before the stop is requested, this
     * is the {@code cap} itself.
     *
     * @param cap the longest time to return
     * @return the time left, between zero and the given {@code cap}
     */
    Duration remaining(Duration cap) {
        return remainingUntil(0, cap);
    }

    /**
     * Returns the time left of the given {@code extension} of the stop deadline: the time until the stop deadline plus
     * the {@code extension}, at most the {@code extension}. Before the stop is requested, this is the
     * {@code extension} itself.
     *
     * @param extension the time beyond the stop deadline
     * @return the time left, between zero and the given {@code extension}
     */
    Duration remainingBeyondDeadline(Duration extension) {
        return remainingUntil(extension.toNanos(), extension);
    }

    private Duration remainingUntil(long beyondDeadline, Duration cap) {
        if (!isRequested()) {
            return cap;
        }
        long remaining = deadline + beyondDeadline - System.nanoTime();
        return Duration.ofNanos(Math.clamp(remaining, 0L, cap.toNanos()));
    }

    /**
     * Waits until the given {@code future} completes, normally or exceptionally, or the given {@code timeout} expires.
     * Once the stop is requested, the wait ends at the stop deadline at the latest.
     *
     * @param future  the future to wait for
     * @param timeout the longest time to wait
     * @return whether the future completed
     */
    boolean await(CompletableFuture<?> future, Duration timeout) {
        long until = System.nanoTime() + timeout.toNanos();
        while (!future.isDone()) {
            boolean stopping = isRequested();
            long end = stopping && deadline - until < 0 ? deadline : until;
            long remaining = end - System.nanoTime();
            if (remaining <= 0) {
                return false;
            }
            // Before the request, the wait also ends when the request arrives, to move to the stop deadline.
            CompletableFuture<?> awaited = stopping ? future : CompletableFuture.anyOf(future, requested);
            try {
                awaited.get(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return future.isDone();
            } catch (ExecutionException | TimeoutException e) {
                // Completed exceptionally, or the time is up; the loop decides.
            }
        }
        return true;
    }
}
