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

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link SnapshotStore} test double that both records how often it was loaded from and stored to, and keeps the
 * stored snapshots in memory. Tests can assert which tenant's store a routing snapshot store delegated to, and that a
 * snapshot stored under one tenant is only loaded back from that same tenant's store.
 */
public class RecordingSnapshotStore implements SnapshotStore {

    private final Map<Object, Snapshot> snapshotsByIdentifier = new ConcurrentHashMap<>();

    private int loadCount;
    private int storeCount;

    /**
     * Returns how often a snapshot was read from {@code this} store, so a test can assert which tenant's store served
     * the read.
     *
     * @return how often {@link #load(QualifiedName, Object, ProcessingContext)} was called
     */
    public int loadCount() {
        return loadCount;
    }

    /**
     * Returns how often a snapshot was written to {@code this} store.
     *
     * @return how often {@link #store(QualifiedName, Object, Snapshot, ProcessingContext)} was called
     */
    public int storeCount() {
        return storeCount;
    }

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot,
                                         @Nullable ProcessingContext context) {
        storeCount++;
        snapshotsByIdentifier.put(identifier, snapshot);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier,
                                                      @Nullable ProcessingContext context) {
        loadCount++;
        return CompletableFuture.completedFuture(snapshotsByIdentifier.get(identifier));
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        // No-op - not required for testing
    }
}
