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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.annotation.TenantComponentParameterResolverFactory;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.RegisterTenantDescriptorHandlerInterceptor;
import io.axoniq.framework.messaging.multitenancy.queryhandling.TenantAwareQueryBus;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBusConfigurationEnhancer;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.messaging.core.interception.HandlerInterceptorRegistry;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryBus;

import static io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled.isEnabled;
import static org.axonframework.common.configuration.DecoratorDefinition.forType;

/**
 * {@link ConfigurationEnhancer} registering the default multi-tenancy components:
 * <ul>
 *     <li>the default {@link TenantResolver}, which resolves the tenant from message metadata, unless a user registered a custom {@link TenantResolver}</li>
 *     <li>the {@link TenantRouter} that every tenant-routing component shares to decide the tenant of a message</li>
 *     <li>the {@link TenantComponentParameterResolverFactory} to inject tenant-scoped components into message handlers</li>
 *     <li>the {@link TenantComponentProviderSubscriber} to subscribe every {@link TenantComponentProvider} to the {@link TenantProvider} at startup</li>
 *     <li>the {@link RegisterTenantDescriptorHandlerInterceptor} which takes the resolved {@link TenantDescriptor} from the message and stores it in the {@link ProcessingContext}</li>
 *     <li>the {@link TenantAwareQueryBus} decorator, scoping subscription-query update emission and completion to the tenant resolved from the {@link ProcessingContext}</li>
 * </ul>
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Theo Emanuelsson
 * @author Jan Galinski
 * @author Laura Devriendt
 * @author Jakob Hatzl
 * @since 5.3.0
 */
@Internal
@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
public class MultiTenancyConfigurationDefaults implements ConfigurationEnhancer {

    /**
     * The order of {@code this} enhancer compared to others.
     * <p>
     * Runs early, so the multi-tenancy defaults registered here are in place before other enhancers and user
     * registrations that build on them.
     */
    public static final int ENHANCER_ORDER = Integer.MIN_VALUE+5;

    /**
     * The lifecycle phase in which the {@link TenantProvider} starts and shuts down.
     * <p>
     * Must start well before {@link Phase#INBOUND_COMMAND_CONNECTOR}, the phase at which the multi-tenant command
     * bus connector starts every per-tenant connector it was subscribed with by then. Starting the
     * {@code TenantProvider} any later would leave it with no known tenants yet, so the connector would start with
     * zero per-tenant connectors to start.
     * <p>
     * Public so that backend-specific enhancers registering a {@link TenantProvider} implementation (e.g.
     * {@link io.axoniq.framework.messaging.multitenancy.axonserver.configuration.AxonServerMultiTenancyConfigurationDefaults})
     * can align their component's start and shutdown phase with this one.
     */
    public static final int TENANT_PROVIDER_PHASE = -10;

    /**
     * The lifecycle phase of the {@link TenantComponentProviderSubscriber}. It starts after the {@link TenantProvider},
     * so the tenants replayed on subscription are complete. Shutdown runs in reverse phase order, so the subscriptions
     * are cancelled while the {@code TenantProvider} is still running.
     * <p>
     * Public so that backend-specific enhancers registering a per-tenant command bus connector (e.g.
     * {@link io.axoniq.framework.messaging.multitenancy.axonserver.configuration.AxonServerMultiTenancyConfigurationDefaults})
     * can subscribe it to the {@link TenantProvider} at the same phase.
     */
    public static final int TENANT_COMPONENT_SUBSCRIBER_PHASE = TENANT_PROVIDER_PHASE + 5;

    /**
     * The order at which {@link TenantAwareQueryBus} decorates the {@code QueryBus}.
     * <p>
     * Must be higher (applied further outside) than
     * {@link DistributedQueryBusConfigurationEnhancer#DISTRIBUTED_QUERY_BUS_ORDER}: the distributed {@code QueryBus}
     * does not delegate update emission or completion to its wrapped local segment, it owns the update registry
     * directly, so a lower-order (inner) placement of {@code TenantAwareQueryBus} would never observe emit or complete
     * calls at all. Staying below {@code InterceptingQueryBus.DECORATION_ORDER} keeps it inside the intercepting layer,
     * whose {@code emitUpdate}/{@code completeSubscriptions*} overrides pass the filter through unmodified regardless.
     */
    public static final int TENANT_AWARE_QUERY_BUS_ORDER =
            DistributedQueryBusConfigurationEnhancer.DISTRIBUTED_QUERY_BUS_ORDER + 25;

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        // TODO: see #258 - find a way that is not user facing but only needed for our mixed-scope itests.
        if (!isEnabled(componentRegistry)) {
            return;
        }

        // Register the default TenantResolver, which resolves the tenant from message metadata.
        componentRegistry.registerIfNotPresent(TenantResolver.class,
                                               c -> new MetadataBasedTenantResolver(),
                                               SearchScope.ALL);

        // Register the TenantRouter, so every tenant-routing component decides the tenant of a message the same way,
        // against one and the same set of known tenants, rather than each building its own.
        componentRegistry.registerIfNotPresent(TenantRouter.class,
                                               config -> new TenantRouter(config.getComponent(TenantResolver.class),
                                                                          config.getComponent(TenantProvider.class)),
                                               SearchScope.ALL);

        // Keep every TenantComponentProvider in sync with the tenants known to the TenantProvider.
        registerTenantComponentProviderSubscription(componentRegistry);

        // Register HandlerInterceptor that puts a ResourceKey with the resolved TenantDescriptor into {@link org.axonframework.messaging.core.unitofwork.ProcessingContext}.
        registerTenantDescriptorInterceptor(componentRegistry);

        // Scope subscription-query update emission and completion to the tenant resolved from the ProcessingContext.
        registerTenantAwareQueryBusDecorator(componentRegistry);
    }

    /**
     * Registers the {@link TenantComponentProviderSubscriber}, subscribing every {@link TenantComponentProvider} to the
     * {@link TenantProvider} at startup, so providers follow the tenant lifecycle: known tenants are replayed on
     * subscription and tenants added or removed at runtime reach every provider. At shutdown the retained subscriptions
     * are cancelled, destroying each tenant's component instances.
     *
     * @param componentRegistry the registry to register the subscriber with
     */
    static void registerTenantComponentProviderSubscription(ComponentRegistry componentRegistry) {
        // A TenantProvider is always present, since this enhancer registers one itself when none is configured.
        componentRegistry.registerComponent(
                ComponentDefinition
                        .ofType(TenantComponentProviderSubscriber.class)
                        .withBuilder(TenantComponentProviderSubscriber::new)
                        .onStart(TENANT_COMPONENT_SUBSCRIBER_PHASE,
                                 TenantComponentProviderSubscriber::subscribeProviders)
                        .onShutdown(TENANT_COMPONENT_SUBSCRIBER_PHASE,
                                    TenantComponentProviderSubscriber::cancelSubscriptions)
        );
    }

    static void registerTenantDescriptorInterceptor(ComponentRegistry componentRegistry) {
        componentRegistry.registerDecorator(
                HandlerInterceptorRegistry.class,
                0,
                (config, name, delegate) -> delegate
                        .registerCommandInterceptor(MultiTenancyConfigurationDefaults::interceptorFactory)
                        .registerQueryInterceptor(MultiTenancyConfigurationDefaults::interceptorFactory)
        );
    }

    /**
     * Decorates the {@code QueryBus} with a {@link TenantAwareQueryBus}, scoping subscription-query update emission
     * and completion to the tenant resolved from the {@link ProcessingContext} and rejecting queries for tenants that
     * are not served.
     *
     * @param componentRegistry the registry to register the decorator with
     */
    static void registerTenantAwareQueryBusDecorator(ComponentRegistry componentRegistry) {
        componentRegistry.registerDecorator(
                forType(QueryBus.class)
                        .with((config, name, delegate) -> delegate instanceof TenantAwareQueryBus
                                ? delegate
                                : new TenantAwareQueryBus(delegate,
                                                          config.getComponent(TenantResolver.class),
                                                          config.getComponent(TenantProvider.class)))
                        .order(TENANT_AWARE_QUERY_BUS_ORDER)
        );
    }

    private static RegisterTenantDescriptorHandlerInterceptor interceptorFactory(Configuration config) {
        return new RegisterTenantDescriptorHandlerInterceptor(config.getComponent(TenantRouter.class));
    }
}
