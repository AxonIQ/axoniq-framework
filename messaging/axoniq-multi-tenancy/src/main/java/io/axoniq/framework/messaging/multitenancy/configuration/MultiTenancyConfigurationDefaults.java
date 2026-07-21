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

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.annotation.TenantComponentParameterResolverFactory;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.RegisterTenantDescriptorHandlerInterceptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.axonserver.AxonServerTenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.axonserver.AxonServerTenantProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.MultiTenantAxonServerCommandBusConnector;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentLifecycleHandler;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.messaging.core.configuration.reflection.ParameterResolverFactoryUtils;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.interception.HandlerInterceptorRegistry;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled.isEnabled;

/**
 * {@link ConfigurationEnhancer} registering the default multi-tenancy components:
 * <ul>
 *     <li>the default {@link TenantResolver}, which resolves the tenant from message metadata, unless a user registered a custom {@link TenantResolver}</li>
 *     <li>the {@link TenantProvider} which is responsible for managing tenants and lifecycle, default to {@link AxonServerTenantProvider}</li>
 *     <li>the {@link TenantComponentParameterResolverFactory} to inject tenant-scoped components into message handlers</li>
 *     <li>the {@link TenantComponentProviderSubscriber} to subscribe every {@link TenantComponentProvider} to the {@link TenantProvider} at startup</li>
 *     <li>the {@link RegisterTenantDescriptorHandlerInterceptor} which takes the resolved {@link TenantDescriptor} from the message and stores it in the {@link ProcessingContext}</li>
 *     <li>the {@link MultiTenantAxonServerCommandBusConnector} which fans out commands to a connector per tenant</li>
 * </ul>
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Theo Emanuelsson
 * @author Jan Galinski
 * @author Laura Devriendt
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
    public static final int ENHANCER_ORDER = Integer.MIN_VALUE + 5;

    /**
     * The lifecycle phase in which the {@link TenantProvider} starts and shuts down.
     * <p>
     * Must start well before {@link Phase#INBOUND_COMMAND_CONNECTOR}, the phase at which the
     * {@link MultiTenantAxonServerCommandBusConnector} starts every per-tenant connector it was subscribed with by
     * then. Starting the {@code TenantProvider} any later would leave it with no known tenants yet, so the connector
     * would start with zero per-tenant connectors to start.
     */
    private static final int TENANT_PROVIDER_PHASE = -10;

    /**
     * The lifecycle phase of the {@link TenantComponentProviderSubscriber}. It starts after the {@link TenantProvider},
     * so the tenants replayed on subscription are complete. Shutdown runs in reverse phase order, so the subscriptions
     * are cancelled while the {@code TenantProvider} is still running.
     */
    private static final int TENANT_COMPONENT_SUBSCRIBER_PHASE = TENANT_PROVIDER_PHASE + 5;

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

        // Register the Axon Server TenantProvider, which is the default implementation for multi-tenancy in Axon Server.
        componentRegistry.registerIfNotPresent(axonServerTenantProvider(), SearchScope.ALL);

        // Keep every TenantComponentProvider in sync with the tenants known to the TenantProvider.
        registerTenantComponentProviderSubscription(componentRegistry);

        // Register HandlerInterceptor that puts a ResourceKey with the resolved TenantDescriptor into {@link org.axonframework.messaging.core.unitofwork.ProcessingContext}.
        registerTenantDescriptorInterceptor(componentRegistry);

        // Register the MultiTenantAxonServerCommandBusConnector
        componentRegistry.registerIfNotPresent(multiTenantCommandBusConnector(), SearchScope.ALL);
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

    /**
     * Provides a {@link ComponentDefinition} for the {@link TenantProvider} that is backed by Axon Server. Uses the
     * {@link AxonServerConnectionManager} to manage connections and the {@link TenantConnectPredicate} to determine
     * which tenants to connect to, defaults to {@link AxonServerTenantConnectPredicate}.
     *
     * @return a {@link ComponentDefinition} for the {@link TenantProvider} that is backed by Axon Server
     */
    public static ComponentDefinition<TenantProvider> axonServerTenantProvider() {
        return ComponentDefinition
                .ofType(TenantProvider.class)
                .withBuilder(config -> new AxonServerTenantProvider(
                                     config.getComponent(AxonServerConnectionManager.class),
                                     config.getComponent(TenantConnectPredicate.class, AxonServerTenantConnectPredicate::new)
                             )
                )
                .onStart(TENANT_PROVIDER_PHASE,
                         (ComponentLifecycleHandler<TenantProvider>) (config, provider) ->
                                 ((AxonServerTenantProvider) provider).start())
                .onShutdown(TENANT_PROVIDER_PHASE,
                            (ComponentLifecycleHandler<TenantProvider>) (config, provider) ->
                                    ((AxonServerTenantProvider) provider).shutdown());
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

    private static RegisterTenantDescriptorHandlerInterceptor interceptorFactory(Configuration config) {
        return new RegisterTenantDescriptorHandlerInterceptor(
                config.getComponent(TenantResolver.class),
                config.getComponent(TenantProvider.class)
        );
    }

    /**
     * Builds the {@link MultiTenantAxonServerCommandBusConnector}, subscribing it to the {@link TenantProvider} at
     * startup so it follows the tenant lifecycle: known tenants are replayed on subscription and tenants added or
     * removed at runtime reach it. Shutdown handlers run in descending phase order, so at shutdown the connector is
     * disconnected first ({@link Phase#INBOUND_COMMAND_CONNECTOR}), then its dispatching is shut down
     * ({@link Phase#OUTBOUND_COMMAND_CONNECTORS}), before the retained tenant-provider subscription is cancelled
     * last.
     * <p>
     * The tenant-provider subscription is retained on the {@code connector} instance received by the lifecycle handlers
     * directly, rather than re-resolved through {@link Configuration#getComponent(Class)} for
     * {@link CommandBusConnector}: other {@link ConfigurationEnhancer ConfigurationEnhancers} may decorate that type
     * (for example to convert payloads), and looking it up again would subscribe the decorator instead of the
     * underlying, multi-tenant aware connector built here.
     *
     * @return a {@link ComponentDefinition} for the {@link MultiTenantAxonServerCommandBusConnector}
     */
    private static ComponentDefinition<CommandBusConnector> multiTenantCommandBusConnector() {
        AtomicReference<@Nullable Registration> tenantSubscription = new AtomicReference<>();
        return ComponentDefinition.ofType(CommandBusConnector.class)
                                  .withBuilder(config -> new MultiTenantAxonServerCommandBusConnector(
                                          config.getComponent(TenantResolver.class),
                                          config.getComponent(AxonServerConnectionManager.class),
                                          config.getComponent(AxonServerConfiguration.class),
                                          config.getComponent(MessageConverter.class)))
                                  .onStart(TENANT_COMPONENT_SUBSCRIBER_PHASE,
                                           (config, connector) -> {
                                               tenantSubscription.set(config.getComponent(TenantProvider.class)
                                                                            .subscribe((MultiTenantAxonServerCommandBusConnector) connector));
                                               return FutureUtils.emptyCompletedFuture();
                                           })
                                  .onStart(Phase.INBOUND_COMMAND_CONNECTOR,
                                           connector -> ((MultiTenantAxonServerCommandBusConnector) connector).start())
                                  .onShutdown(Phase.OUTBOUND_COMMAND_CONNECTORS,
                                              (ComponentLifecycleHandler<CommandBusConnector>) (config, connector) ->
                                                      ((MultiTenantAxonServerCommandBusConnector) connector).shutdownDispatching())
                                  .onShutdown(Phase.INBOUND_COMMAND_CONNECTOR,
                                              (ComponentLifecycleHandler<CommandBusConnector>) (config, connector) ->
                                                      ((MultiTenantAxonServerCommandBusConnector) connector).disconnect())
                                  .onShutdown(TENANT_COMPONENT_SUBSCRIBER_PHASE,
                                              (config, connector) -> {
                                                  Registration registration = tenantSubscription.get();
                                                  if (registration != null) {
                                                      registration.cancel();
                                                  }
                                                  return FutureUtils.emptyCompletedFuture();
                                              });
    }
}
