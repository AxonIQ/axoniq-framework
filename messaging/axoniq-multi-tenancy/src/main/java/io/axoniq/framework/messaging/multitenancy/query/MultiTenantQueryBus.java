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

package io.axoniq.framework.messaging.multitenancy.query;

import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.NoSuchTenantException;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.query.TenantQuerySegmentFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.Objects;

/**
 * Implementation of a {@link QueryBus} that is aware of multiple tenant instances of a {@code QueryBus}. Each
 * {@code QueryBus} instance is considered a "tenant".
 * <p>
 * The {@code MultiTenantQueryBus} relies on a {@link TenantResolver} to dispatch queries via the resolved tenant
 * segment of the {@code QueryBus}. {@link TenantQuerySegmentFactory} is used as a factory to create tenant segments.
 *
 * @author Jan Galinski
 * @since 5.0.0
 */
public class MultiTenantQueryBus implements QueryBus, MultiTenantAwareComponent {

    private final Map<QualifiedName, QueryHandler> handlers = new ConcurrentHashMap<>();
    private final Map<TenantDescriptor, QueryBus> tenantSegments = new ConcurrentHashMap<>();

    private final TenantQuerySegmentFactory tenantSegmentFactory;
    private final TenantResolver<Message> tenantResolver;

    /**
     * Instantiate a MultiTenantQueryBus.
     *
     * @param tenantSegmentFactory the factory to create tenant segments
     * @param tenantResolver       the resolver to resolve the tenant descriptor for a given message
     */
    public MultiTenantQueryBus(TenantQuerySegmentFactory tenantSegmentFactory,
                               TenantResolver<Message> tenantResolver) {
        this.tenantSegmentFactory = tenantSegmentFactory;
        this.tenantResolver = tenantResolver;
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query,
                                                     @Nullable ProcessingContext processingContext) {
        return resolveTenant(query).query(query, processingContext);
    }

    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                 @Nullable ProcessingContext processingContext,
                                                                 int updateBufferSize) {
        return resolveTenant(query).subscriptionQuery(query, processingContext, updateBufferSize);
    }

    @Override
    public MessageStream<SubscriptionQueryUpdateMessage> subscribeToUpdates(QueryMessage query, int updateBufferSize) {
        return resolveTenant(query).subscribeToUpdates(query, updateBufferSize);
    }

    @Override
    public CompletableFuture<Void> emitUpdate(Predicate<QueryMessage> filter,
                                              Supplier<SubscriptionQueryUpdateMessage> updateSupplier,
                                              @Nullable ProcessingContext processingContext) {
        return resolveTenantFromProcessingContext(processingContext).emitUpdate(filter, updateSupplier, processingContext);
    }

    @Override
    public CompletableFuture<Void> completeSubscriptions(Predicate<QueryMessage> filter,
                                                         @Nullable ProcessingContext processingContext) {
        return resolveTenantFromProcessingContext(processingContext).completeSubscriptions(filter, processingContext);
    }

    @Override
    public CompletableFuture<Void> completeSubscriptionsExceptionally(Predicate<QueryMessage> filter,
                                                                       Throwable cause,
                                                                       @Nullable ProcessingContext processingContext) {
        return resolveTenantFromProcessingContext(processingContext)
                .completeSubscriptionsExceptionally(filter, cause, processingContext);
    }

    @Override
    public QueryBus subscribe(QualifiedName queryName, QueryHandler queryHandler) {
        handlers.computeIfAbsent(queryName, k -> {
            tenantSegments.forEach((tenant, segment) -> segment.subscribe(queryName, queryHandler));
            return queryHandler;
        });
        return this;
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, tenantSegmentFactory);

        return unregisterTenantOnCancel(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, tenant -> {
            QueryBus tenantSegment = tenantSegmentFactory.apply(tenant);
            handlers.forEach(tenantSegment::subscribe);
            return tenantSegment;
        });

        return unregisterTenantOnCancel(tenantDescriptor);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenantSegments", tenantSegments);
    }

    private QueryBus resolveTenant(QueryMessage queryMessage) {
        TenantDescriptor tenantDescriptor = tenantResolver.resolveTenant(queryMessage, tenantSegments.keySet());
        QueryBus tenantQueryBus = tenantSegments.get(tenantDescriptor);
        if (tenantQueryBus == null) {
            throw new NoSuchTenantException(tenantDescriptor.tenantId());
        }
        return tenantQueryBus;
    }

    private QueryBus resolveTenantFromProcessingContext(ProcessingContext processingContext) {
        Objects.requireNonNull(processingContext, "ProcessingContext must not be null.");
        return resolveTenant((QueryMessage) Message.fromContext(processingContext));
    }

    private Registration unregisterTenantOnCancel(TenantDescriptor tenantDescriptor) {
        return () -> tenantSegments.remove(tenantDescriptor) != null;
    }
}
