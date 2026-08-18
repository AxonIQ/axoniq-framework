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
package io.axoniq.workflow.runtime.api.execution.context;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * The append condition of one workflow instance: it holds the position in the event store the instance last wrote
 * at, and hands it to the next append so the store can reject a writer that fell behind.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
@Internal
public interface WorkflowAppendCondition {

    /**
     * Runs the given {@code append} once the previous append of this instance has finished, then keeps the position
     * it reports for the next one.
     * <p>
     * The {@code append} receives the position the instance last wrote at, or {@code null} when it has written nothing
     * yet, and returns the position it wrote at. A failed append leaves the position of the previous one in place, so
     * the next append still conditions from the last event that was actually written.
     *
     * @param append appends one event from the given position and returns the position it wrote at
     * @return a future completing once the append has finished
     */
    CompletableFuture<Void> appendSequentially(
            Function<ConsistencyMarker, CompletableFuture<ConsistencyMarker>> append
    );

    /**
     * Updates the position the next append conditions from, keeping the further of the two. Used when an instance is
     * restored, so its first append after that conditions from where it had already written.
     *
     * @param position the position the instance last wrote at, or {@code null} when unknown
     */
    void updateAppendPosition(@Nullable ConsistencyMarker position);
}
