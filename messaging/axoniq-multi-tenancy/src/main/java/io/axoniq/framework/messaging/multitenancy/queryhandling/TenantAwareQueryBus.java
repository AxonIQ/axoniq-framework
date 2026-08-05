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
package io.axoniq.framework.messaging.multitenancy.queryhandling;

import io.axoniq.framework.messaging.multitenancy.MultiTenancyAxoniqAddon;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptors;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryUpdateEmitter;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * {@link QueryBus} decorator scoping subscription-query update emission and completion to the tenant resolved from the
 * current {@link ProcessingContext}.
 * <p>
 * Subscription-query dispatch is already tenant-routed by the per-tenant {@code QueryBusConnector} composition (see
 * {@code MultiTenantAxonServerQueryBusConnector}), but update emission is not: a distributed {@code QueryBus} tracks
 * every active subscription in a single registry and matches
 * {@link #emitUpdate(Predicate, Supplier, ProcessingContext)}/
 * {@link #completeSubscriptions(Predicate, ProcessingContext)}/
 * {@link #completeSubscriptionsExceptionally(Predicate, Throwable, ProcessingContext)} calls against all of them,
 * regardless of tenant. This decorator closes that gap by ANDing a tenant clause onto the caller's {@code filter}
 * before delegating, so a matching {@link QueryMessage} only fires when its {@code tenantId} metadata equals the tenant
 * resolved from the given {@code context}. Because {@link QueryUpdateEmitter#forContext(ProcessingContext)} always
 * resolves its {@code QueryBus} from the context, every emitter obtained either by parameter injection or by a direct
 * {@code forContext} call is scoped this way, regardless of how it was constructed.
 * <p>
 * {@link #query(QueryMessage, ProcessingContext)} additionally rejects queries for a tenant that is not (or no longer)
 * served, before delegating. A distributed {@code QueryBus} may serve a query from its local segment whenever a local
 * handler is subscribed for it, bypassing the tenant-routing connector entirely, which would otherwise answer queries
 * for a tenant that was never registered or whose context has been removed. Validating here keeps the outcome identical
 * regardless of whether the query is served locally or dispatched through the connector.
 * <p>
 * The remaining dispatching methods ({@link #subscriptionQuery(QueryMessage, ProcessingContext, int)},
 * {@link #subscribeToUpdates(QueryMessage, int)}) and {@link #subscribe(QualifiedName, QueryHandler)} are pure
 * pass-through: subscription queries always travel through the connector, which resolves the tenant itself.
 * <p>
 * Registered as a decorator on {@link QueryBus} by
 * {@link io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults}, positioned
 * between {@code DistributedQueryBus} and {@code InterceptingQueryBus} in the decoration chain. Not intended to be
 * instantiated directly by applications.
 *
 * @author Jakob Hatzl
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class TenantAwareQueryBus implements QueryBus {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final QueryBus delegate;
    private final TenantResolver tenantResolver;
    private final TenantDescriptors tenantDescriptors;

    /**
     * Constructs a {@code TenantAwareQueryBus}, delegating all operations to the given {@code delegate}.
     *
     * @param delegate          the {@code QueryBus} to delegate all operations to
     * @param tenantResolver    the {@link TenantResolver} used for tenant resolution from query messages
     * @param tenantDescriptors the currently served {@link TenantDescriptor TenantDescriptors}, used to reject queries
     *                          for tenants that are not served
     */
    public TenantAwareQueryBus(QueryBus delegate,
                               TenantResolver tenantResolver,
                               TenantDescriptors tenantDescriptors) {
        this.delegate = requireNonNull(delegate, "The QueryBus delegate must not be null.");
        this.tenantResolver = requireNonNull(tenantResolver, "The TenantResolver must not be null.");
        this.tenantDescriptors = requireNonNull(tenantDescriptors, "The TenantDescriptors must not be null.");
        EntitlementManager.INSTANCE.registerAddon(MultiTenancyAxoniqAddon.class);
    }

    @Override
    public TenantAwareQueryBus subscribe(QualifiedName queryName, QueryHandler queryHandler) {
        delegate.subscribe(queryName, queryHandler);
        return this;
    }

    /**
     * Dispatches the given {@code query} for the tenant resolved from it.
     * <p>
     * Rejects the {@code query} with a {@link TenantNotResolvedException} when its
     * {@link TenantResolver#resolveTenant(Message, Collection) resolved tenant} is not among the currently served
     * tenants, rather than leaving that verdict to the tenant-routing connector: a distributed {@code QueryBus} serves
     * queries with a locally subscribed handler from its local segment, never reaching the connector.
     *
     * @param query   the query to dispatch
     * @param context the processing context under which the query is dispatched (can be {@code null})
     * @return a {@link MessageStream} of the responses for the given {@code query}
     * @throws TenantNotResolvedException if no tenant can be resolved from the given {@code query}, or the resolved
     *                                    tenant is not served
     */
    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
        assertServedTenant(query);
        return delegate.query(query, context);
    }

    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                 @Nullable ProcessingContext context,
                                                                 int updateBufferSize) {
        return delegate.subscriptionQuery(query, context, updateBufferSize);
    }

    @Override
    public MessageStream<SubscriptionQueryUpdateMessage> subscribeToUpdates(QueryMessage query, int updateBufferSize) {
        return delegate.subscribeToUpdates(query, updateBufferSize);
    }

    /**
     * Emits the outcome of the {@code updateSupplier} to
     * {@link QueryBus#subscriptionQuery(QueryMessage, ProcessingContext, int) subscription queries} matching the given
     * {@code queryName} and given {@code filter}.
     * <p>
     * {@code AND}s a tenant clause onto the given {@code filter}, matching only {@link QueryMessage QueryMessages}
     * whose {@link TenantResolver#resolveTenant(Message) resolved tenant} equals the tenant resolved from the given
     * {@code context}.
     *
     * @param filter         a predicate filtering on {@link QueryMessage QueryMessages}; the {@code updateSupplier} is
     *                       only sent to subscription queries matching this filter
     * @param updateSupplier the update supplier to emit for
     *                       {@link QueryBus#subscriptionQuery(QueryMessage, ProcessingContext, int) subscription
     *                       queries} matching the given {@code filter}
     * @param context        the processing context under which the updateSupplier is being emitted; this is needed to
     *                       resolve the actual tenant; a {@code null} context submitted here will produce a
     *                       {@link TenantNotResolvedException} immediately
     * @return a future completing whenever the updateSupplier has been emitted
     * @throws TenantNotResolvedException in case no context is supplied
     */
    @Override
    public CompletableFuture<Void> emitUpdate(Predicate<QueryMessage> filter,
                                              Supplier<SubscriptionQueryUpdateMessage> updateSupplier,
                                              @Nullable ProcessingContext context) {
        return delegate.emitUpdate(scopedToTenant(context, filter), updateSupplier, context);
    }

    /**
     * Emits the outcome of the {@code updateSupplier} to
     * {@link QueryBus#subscriptionQuery(QueryMessage, ProcessingContext, int) subscription queries} matching the given
     * {@code queryName} and given {@code filter}, returning the number of subscription queries the update was emitted
     * to.
     * <p>
     * Implementations that cannot determine this number return {@link OptionalInt#empty} instead.
     * <p>
     * {@code AND}s a tenant clause onto the given {@code filter}, matching only {@link QueryMessage QueryMessages}
     * whose {@link TenantResolver#resolveTenant(Message) resolved tenant} equals the tenant resolved from the given
     * {@code context}.
     *
     * @param filter         a predicate filtering on {@link QueryMessage QueryMessages}. The {@code updateSupplier}
     *                       will only be sent to subscription queries matching this filter
     * @param updateSupplier the update supplier to emit for
     *                       {@link QueryBus#subscriptionQuery(QueryMessage, ProcessingContext, int) subscription
     *                       queries} matching the given {@code filter}
     * @param context        the processing context under which the updateSupplier is being emitted; this is needed to
     *                       resolve the actual tenant; a {@code null} context submitted here will produce a
     *                       {@link TenantNotResolvedException} immediately
     * @return a future completing with the number of subscription queries the update was emitted to as an
     * {@link OptionalInt}, which is empty when we couldn't match
     * @throws TenantNotResolvedException in case no context is supplied
     */
    @Override
    public CompletableFuture<OptionalInt> emitUpdateAndCount(Predicate<QueryMessage> filter,
                                                             Supplier<SubscriptionQueryUpdateMessage> updateSupplier,
                                                             @Nullable ProcessingContext context) {
        return delegate.emitUpdateAndCount(scopedToTenant(context, filter), updateSupplier, context);
    }

    /**
     * Completes {@link QueryBus#subscriptionQuery(QueryMessage, ProcessingContext, int) subscription queries} matching
     * the given {@code filter}.
     * <p>
     * To be used whenever there are no subsequent update to
     * {@link #emitUpdate(Predicate, Supplier, ProcessingContext) emit} left.
     * <p>
     * {@code AND}s a tenant clause onto the given {@code filter}, matching only {@link QueryMessage QueryMessages}
     * whose {@link TenantResolver#resolveTenant(Message) resolved tenant} equals the tenant resolved from the given
     * {@code context}.
     *
     * @param filter  a predicate filtering on {@link QueryMessage QueryMessages}; subscription queries matching this
     *                filter are completed
     * @param context the processing context within which to complete subscription queries (can be {@code null});
     *                this is needed to resolve the actual tenant; a {@code null} context submitted here will produce a
     *                {@link TenantNotResolvedException} immediately
     * @return a future completing whenever all matching
     * {@link QueryBus#subscriptionQuery(QueryMessage, ProcessingContext, int) subscription queries} have been
     * completed
     * @throws TenantNotResolvedException in case no context is supplied
     */
    @Override
    public CompletableFuture<Void> completeSubscriptions(Predicate<QueryMessage> filter,
                                                         @Nullable ProcessingContext context) {
        return delegate.completeSubscriptions(scopedToTenant(context, filter), context);
    }

    /**
     * Completes {@link QueryBus#subscriptionQuery(QueryMessage, ProcessingContext, int) subscription queries} matching
     * the given {@code filter}, returning the number of subscription queries that were completed.
     * <p>
     * Implementations that cannot determine this number return {@link OptionalInt#empty} instead.
     * <p>
     * {@code AND}s a tenant clause onto the given {@code filter}, matching only {@link QueryMessage QueryMessages}
     * whose {@link TenantResolver#resolveTenant(Message) resolved tenant} equals the tenant resolved from the given
     * {@code context}.
     *
     * @param filter  a predicate filtering on {@link QueryMessage QueryMessages}. Subscription queries matching this
     *                filter will be completed
     * @param context the processing context within which to complete subscription queries (can be {@code null}); this
     *                is needed to resolve the actual tenant; a {@code null} context submitted here will produce a
     *                {@link TenantNotResolvedException} immediately
     * @return a future completing with the number of subscription queries that were completed as an
     * {@link OptionalInt}, which is empty when we couldn't match
     * @throws TenantNotResolvedException in case no context is supplied
     */
    @Override
    public CompletableFuture<OptionalInt> completeSubscriptionsAndCount(Predicate<QueryMessage> filter,
                                                                        @Nullable ProcessingContext context) {
        return delegate.completeSubscriptionsAndCount(scopedToTenant(context, filter), context);
    }

    /**
     * Completes {@link QueryBus#subscriptionQuery(QueryMessage, ProcessingContext, int) subscription queries} matching
     * the given {@code filter} exceptionally with the given {@code cause}.
     * <p>
     * To be used whenever {@link #emitUpdate(Predicate, Supplier, ProcessingContext) emitting update} should be stopped
     * due to some exception.
     * <p>
     * {@code AND}s a tenant clause onto the given {@code filter}, matching only {@link QueryMessage QueryMessages}
     * whose {@link TenantResolver#resolveTenant(Message) resolved tenant} equals the tenant resolved from the given
     * {@code context}.
     *
     * @param filter  a predicate filtering on {@link QueryMessage QueryMessages}; subscription queries matching this
     *                filter are completed exceptionally
     * @param cause   the cause of an error
     * @param context the processing context within which to complete subscription queries exceptionally (can be
     *                {@code null}); this is needed to resolve the actual tenant; a {@code null} context submitted
     *                here will produce a {@link TenantNotResolvedException} immediately
     * @return a future completing whenever all matching
     * {@link QueryBus#subscriptionQuery(QueryMessage, ProcessingContext, int) subscription queries} have been completed
     * exceptionally
     * @throws TenantNotResolvedException in case no context is supplied
     */
    @Override
    public CompletableFuture<Void> completeSubscriptionsExceptionally(Predicate<QueryMessage> filter,
                                                                      Throwable cause,
                                                                      @Nullable ProcessingContext context) {
        return delegate.completeSubscriptionsExceptionally(scopedToTenant(context, filter), cause, context);
    }

    /**
     * Completes {@link QueryBus#subscriptionQuery(QueryMessage, ProcessingContext, int) subscription queries} matching
     * the given {@code filter} exceptionally with the given {@code cause}, returning the number of subscription queries
     * that were completed exceptionally.
     * <p>
     * Implementations that cannot determine this number return {@link OptionalInt#empty} instead.
     * <p>
     * {@code AND}s a tenant clause onto the given {@code filter}, matching only {@link QueryMessage QueryMessages}
     * whose {@link TenantResolver#resolveTenant(Message) resolved tenant} equals the tenant resolved from the given
     * {@code context}.
     *
     * @param filter  a predicate filtering on {@link QueryMessage QueryMessages}. Subscription queries matching this
     *                filter will be completed exceptionally
     * @param cause   the cause of an error
     * @param context the processing context within which to complete subscription queries exceptionally (can be
     *                {@code null}); this is needed to resolve the actual tenant; a {@code null} context submitted here
     *                will produce a {@link TenantNotResolvedException} immediately
     * @return a future completing with the number of subscription queries that were completed exceptionally as an
     * {@link OptionalInt}, which is empty when we couldn't match
     * @throws TenantNotResolvedException in case no context is supplied
     */
    @Override
    public CompletableFuture<OptionalInt> completeSubscriptionsExceptionallyAndCount(Predicate<QueryMessage> filter,
                                                                                     Throwable cause,
                                                                                     @Nullable ProcessingContext context) {
        return delegate.completeSubscriptionsExceptionallyAndCount(scopedToTenant(context, filter), cause, context);
    }

    /**
     * {@code AND}s a tenant clause onto the given {@code filter}, matching only {@link QueryMessage QueryMessages}
     * whose {@link TenantResolver#resolveTenant(Message) resolved tenant} equals the tenant resolved from the given
     * {@code context}.
     * <p>
     * A {@link QueryMessage} for which the {@code tenantResolver} cannot resolve a tenant is treated as a non-match
     * rather than propagating: the returned filter is tested once per entry of a shared, multi-tenant update registry
     * (see {@code DistributedQueryBus#emitUpdate}), so letting a resolution failure escape for one unrelated entry
     * would abort matching for every other entry tested in the same call.
     *
     * @param context the processing context to resolve the tenant from
     * @param filter  the caller-supplied filter to scope to the resolved tenant
     * @return a filter matching only {@code QueryMessages} of the resolved tenant that also match the given
     * {@code filter}
     * @throws TenantNotResolvedException if the given {@code context} is {@code null} or carries no tenant
     */
    private Predicate<QueryMessage> scopedToTenant(@Nullable ProcessingContext context,
                                                   Predicate<QueryMessage> filter) {
        if (context == null) {
            throw new TenantNotResolvedException("Cannot resolve tenant: no ProcessingContext was provided");
        }
        TenantDescriptor tenant = TenantDescriptor.fromContext(context)
                                                  .orElseThrow(() -> new TenantNotResolvedException(
                                                          "Cannot resolve tenant: no ProcessingContext was provided"));
        return message -> isForTenant(tenant, message) && filter.test(message);
    }

    /**
     * Asserts the tenant resolved from the given {@code query} is among the currently served tenants.
     * <p>
     * Matches on {@link TenantDescriptor#tenantId()} rather than on descriptor equality, as a {@link TenantResolver} is
     * free to construct a descriptor of its own instead of returning one of the served ones.
     *
     * @param query the query to resolve and verify the tenant of
     * @throws TenantNotResolvedException if no tenant can be resolved from the given {@code query}, or the resolved
     *                                    tenant is not served
     */
    private void assertServedTenant(QueryMessage query) {
        List<TenantDescriptor> tenants = tenantDescriptors.tenants();
        TenantDescriptor tenant = tenantResolver.resolveTenant(query, tenants);
        if (tenants.stream().noneMatch(served -> served.tenantId().equals(tenant.tenantId()))) {
            logger.warn("Tenant [{}] resolved for query [{}] with identifier [{}] is not served. Rejecting it.",
                        tenant.tenantId(), query.type().qualifiedName(), query.identifier());
            throw TenantNotResolvedException.forTenantId(tenant.tenantId());
        }
    }

    private boolean isForTenant(TenantDescriptor tenant, QueryMessage message) {
        try {
            return tenantResolver.resolveTenant(message).equals(tenant);
        } catch (TenantNotResolvedException e) {
            logger.warn("Could not resolve a tenant for query [{}] with identifier [{}] while matching it against "
                                + "tenant [{}] for emitting a subscription query update or completion. Skipping it.",
                        message.type().qualifiedName(), message.identifier(), tenant.tenantId(), e);
            return false;
        }
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
    }
}
