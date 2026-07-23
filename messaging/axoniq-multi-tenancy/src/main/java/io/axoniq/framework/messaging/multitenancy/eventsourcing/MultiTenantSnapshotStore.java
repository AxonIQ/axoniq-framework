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

import io.axoniq.framework.messaging.multitenancy.api.RoutingTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptors;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantSnapshotStoreFactory;
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
 *
 * @author Jakob Hatzl
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class MultiTenantSnapshotStore implements SnapshotStore, DescribableComponent {

    private final TenantSnapshotStoreFactory snapshotStoreFactory;
    private final RoutingTenantResolver tenantResolver;

    /**
     * Constructs a {@code MultiTenantSnapshotStore}.
     *
     * @param snapshotStoreFactory the factory providing each tenant's {@link SnapshotStore}
     * @param tenantResolver       the resolver determining the tenant of a message
     * @param tenants              the known tenants, used to resolve a message against
     */
    public MultiTenantSnapshotStore(TenantSnapshotStoreFactory snapshotStoreFactory,
                                    TenantResolver tenantResolver,
                                    TenantDescriptors tenants) {
        this.snapshotStoreFactory = requireNonNull(snapshotStoreFactory, "The snapshot store factory must not be null");
        this.tenantResolver = new RoutingTenantResolver(tenantResolver, tenants);
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
        return tenantResolver.resolveFromContext(context)
                .orElseThrow(tenantNotResolved("Snapshot operations require a tenant-carrying processing context"));
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("snapshotStoreFactory", snapshotStoreFactory);
        descriptor.describeProperty("tenantResolver", tenantResolver);
    }
}
