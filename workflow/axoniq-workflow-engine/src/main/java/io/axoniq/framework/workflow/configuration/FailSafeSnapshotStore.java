/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * A {@link SnapshotStore} that reports a failure to store a snapshot through the returned future, also when the
 * delegate throws it instead of returning a failed future.
 * <p>
 * Axon Framework's snapshotting entity module stores a snapshot while it sources the entity. It logs a store failure
 * reported through the future, but a store that throws, for example because it converts the snapshot before returning
 * its future, fails the load of the entity. Snapshots of workflow entities are an optimization, so a failure to store
 * one must never fail the load.
 * <p>
 * Internal, because {@link WorkflowEntityModule} only uses it for the snapshots of the workflow engine's entities.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
final class FailSafeSnapshotStore implements SnapshotStore {

    private final SnapshotStore delegate;

    /**
     * Creates a {@code FailSafeSnapshotStore} storing snapshots in the given {@code delegate}.
     *
     * @param delegate the {@link SnapshotStore} to store snapshots in
     */
    FailSafeSnapshotStore(SnapshotStore delegate) {
        this.delegate = requireNonNull(delegate, "The delegate SnapshotStore must not be null.");
    }

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName,
                                         Object identifier,
                                         Snapshot snapshot,
                                         @Nullable ProcessingContext context) {
        try {
            return delegate.store(qualifiedName, identifier, snapshot, context);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName,
                                                      Object identifier,
                                                      @Nullable ProcessingContext context) {
        return delegate.load(qualifiedName, identifier, context);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
    }
}
