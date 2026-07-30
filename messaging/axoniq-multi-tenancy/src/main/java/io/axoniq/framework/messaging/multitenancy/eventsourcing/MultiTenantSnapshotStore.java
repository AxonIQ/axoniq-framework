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

package io.axoniq.framework.messaging.multitenancy.eventsourcing;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

import static io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException.tenantNotResolved;
import static java.util.Objects.requireNonNull;

/**
 * Tenant-routing {@link SnapshotStore}. Snapshot {@link #load(QualifiedName, Object, ProcessingContext) load} and
 * {@link #store(QualifiedName, Object, Snapshot, ProcessingContext) store} operations are routed to the store of the
 * one tenant resolved from the {@link ProcessingContext}, so each tenant's snapshots live in its own store.
 * <p>
 * Snapshot operations therefore require a tenant-carrying processing context. When none is available, or the tenant
 * cannot be resolved from it, the operation completes exceptionally.
 * <p>
 * Registered as the application's {@link SnapshotStore}, so snapshots written for an entity land in the store of the
 * tenant the entity belongs to. Snapshot reads while sourcing do not travel through here: those stay with
 * {@link MultiTenantEventStorageEngine}, which routes them to the tenant's own engine.
 *
 * @author Jakob Hatzl
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class MultiTenantSnapshotStore implements SnapshotStore, DescribableComponent {

    private final TenantSnapshotStoreFactory snapshotStoreFactory;
    private final TenantRouter tenantRouter;

    /**
     * Constructs a {@code MultiTenantSnapshotStore}.
     *
     * @param snapshotStoreFactory the factory providing each tenant's {@link SnapshotStore}
     * @param tenantRouter         the router deciding which tenant a snapshot operation is routed to
     */
    public MultiTenantSnapshotStore(TenantSnapshotStoreFactory snapshotStoreFactory, TenantRouter tenantRouter) {
        this.snapshotStoreFactory = requireNonNull(snapshotStoreFactory, "The snapshot store factory must not be null");
        this.tenantRouter = requireNonNull(tenantRouter, "The tenant router must not be null");
    }

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot,
                                         @Nullable ProcessingContext context) {
        try {
            TenantDescriptor tenant = tenantFor(context);
            return snapshotStoreFactory.storeFor(tenant).store(qualifiedName, identifier, snapshot, context);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier,
                                                      @Nullable ProcessingContext context) {
        try {
            TenantDescriptor tenant = tenantFor(context);
            return snapshotStoreFactory.storeFor(tenant).load(qualifiedName, identifier, context);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private TenantDescriptor tenantFor(@Nullable ProcessingContext context) {
        return tenantRouter.resolveFromContext(context)
                .orElseThrow(tenantNotResolved("Snapshot operations require a tenant-carrying processing context"));
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("snapshotStoreFactory", snapshotStoreFactory);
        descriptor.describeProperty("tenantRouter", tenantRouter);
    }
}
