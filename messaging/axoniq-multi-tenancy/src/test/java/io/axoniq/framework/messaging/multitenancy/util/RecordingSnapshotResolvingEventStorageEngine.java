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

package io.axoniq.framework.messaging.multitenancy.util;

import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * An {@link EventStorageEngine} test double that resolves snapshots itself, standing in for an engine such as
 * {@code PostgresqlEventStorageEngine}.
 * <p>
 * It is its own {@link SnapshotStore}, backed by a {@link RecordingSnapshotStore}, so a tenant factory returning it as
 * both the engine and the snapshot store leaves it undecorated. When sourced with the
 * {@link SourcingStrategy.Snapshot} strategy it reads the snapshot of the requested identifier and leads its stream
 * with a {@link SnapshotEventMessage}, in a single call.
 * <p>
 * Extends {@link RecordingEventStorageEngine} for the recording it shares with it, so
 * {@link #sourcedWithSnapshotStrategy()} tells whether that strategy reached this engine at all, which is what proves
 * it was not consumed above the tenant fan-out.
 */
public class RecordingSnapshotResolvingEventStorageEngine extends RecordingEventStorageEngine implements SnapshotStore {

    private final RecordingSnapshotStore snapshots = new RecordingSnapshotStore();

    /**
     * @return how often a snapshot was read, whether through {@link #load(QualifiedName, Object, ProcessingContext)} or
     * within a snapshot sourcing
     */
    public int loadCount() {
        return snapshots.loadCount();
    }

    /**
     * @return how often {@link #store(QualifiedName, Object, Snapshot, ProcessingContext)} was called
     */
    public int storeCount() {
        return snapshots.storeCount();
    }

    @Override
    public MessageStream<EventMessage> source(SourcingCondition condition, @Nullable ProcessingContext context) {
        MessageStream<EventMessage> sourced = super.source(condition, context);
        if (condition.strategy() instanceof SourcingStrategy.Snapshot snapshotStrategy) {
            Snapshot snapshot = snapshots.load(snapshotStrategy.qualifiedName(),
                                              snapshotStrategy.identifier(),
                                              context)
                                        .orTimeout(5, TimeUnit.SECONDS)
                                        .join();
            if (snapshot != null) {
                return MessageStream.just(new SnapshotEventMessage(snapshot));
            }
        }
        return sourced;
    }

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot,
                                         @Nullable ProcessingContext context) {
        return snapshots.store(qualifiedName, identifier, snapshot, context);
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier,
                                                      @Nullable ProcessingContext context) {
        return snapshots.load(qualifiedName, identifier, context);
    }
}
