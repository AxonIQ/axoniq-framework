/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.eventsourcing.eventstore;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Represents an empty append transaction. This transaction does nothing and always succeeds. It is used when there are
 * no events to persist.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
public record EmptyAppendTransaction() implements EventStorageEngine.AppendTransaction<Void> {

    /**
     * The single instance of the {@code EmptyAppendTransaction}.
     */
    public static final EventStorageEngine.AppendTransaction<Void> INSTANCE = new EmptyAppendTransaction();

    @Override
    public CompletableFuture<Void> commit() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void rollback() {
        // No action needed
    }

    /**
     * Always provides the consistency marker {@link ConsistencyMarker#ORIGIN}.
     *
     * @param commitResult The result returned from the commit call.
     * @return An empty always a completed future with the consistency marker {@link ConsistencyMarker#ORIGIN}.
     */
    @Override
    public CompletableFuture<ConsistencyMarker> afterCommit(@Nullable Void commitResult) {
        return CompletableFuture.completedFuture(ConsistencyMarker.ORIGIN);
    }
}
