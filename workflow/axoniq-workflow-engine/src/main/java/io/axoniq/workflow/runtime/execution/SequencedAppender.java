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

import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Holds the position one workflow instance last wrote at, and runs its appends one after another.
 * <p>
 * Each append starts only once the previous one has finished, so it is handed the position that one wrote at. Steps of
 * one instance can finish at the same time; letting their appends overlap would hand both of them the same position,
 * and the store would reject the second as if another writer had taken the instance over.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal final class SequencedAppender {

    private final AtomicReference<CompletableFuture<Void>> previousAppend =
            new AtomicReference<>(FutureUtils.emptyCompletedFuture());
    private final AtomicReference<ConsistencyMarker> marker = new AtomicReference<>();

    /**
     * Queues an append after the preceding append and supplies the marker that preceding successful append produced.
     * <p>
     * Multiple step completions of one workflow can request an append concurrently. This method gives each request a
     * distinct turn and passes it the workflow's latest marker, so a successful append advances the position for the
     * next request. A failed append completes its own future exceptionally but releases the next request, which can
     * then use the last marker known to be durable.
     *
     * @param append appends using the current marker and completes with the position committed by that append
     * @return a future completing when the queued append has advanced the marker or exceptionally when it failed
     */
    public CompletableFuture<Void> appendSequentially(
            Function<ConsistencyMarker, CompletableFuture<ConsistencyMarker>> append
    ) {
        var thisAppend = new CompletableFuture<Void>();
        // Take the place of the last append in line, and start once the one it replaced has finished.
        var previous = previousAppend.getAndSet(thisAppend);
        var result = previous.handle((ignored, previousFailure) -> append.apply(marker.get()))
                             .thenCompose(position -> position)
                             .thenAccept(this::updateMarker);
        // Hand over the line either way: a failed append must not stall the appends waiting behind it.
        result.whenComplete((ignored, failure) -> thisAppend.complete(null));
        return result;
    }

    /**
     * Seeds or advances the marker from which the next append is conditionally evaluated.
     * <p>
     * Restoration seeds the marker with the position at which the workflow's own sourcing read finished. Successful
     * appends then advance it to their committed positions. Origin and absent positions are deliberately ignored so a
     * newly created workflow keeps its first append anchored at origin.
     *
     * @param position position observed during restoration or committed by a successful append
     */
    public void updateMarker(@Nullable ConsistencyMarker position) {
        if (position == null || ConsistencyMarker.ORIGIN.equals(position)) {
            return;
        }
        marker.accumulateAndGet(position,
                                (current, next) -> current == null ? next : current.upperBound(next));
    }
}
