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
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngineFactory;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamMessageSource;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentRegistry;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantEventSegmentFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantPersistentStreamMessageSourceFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolverRegistry;
import io.axoniq.framework.messaging.multitenancy.commandhandling.MultiTenantAxonServerCommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantRoutingEventStore;
import io.axoniq.framework.messaging.multitenancy.query.MultiTenantAxonServerQueryBusConnector;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.common.StringUtils;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageDispatchInterceptor;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.configuration.reflection.ParameterResolverFactoryUtils;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.correlation.CorrelationDataProviderRegistry;
import org.axonframework.messaging.core.correlation.SimpleCorrelationDataProvider;
import org.axonframework.messaging.core.interception.DispatchInterceptorRegistry;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.concurrent.CompletableFuture;

/**
 * FIXME: copied docs, needs rewrite
 * {@link ConfigurationEnhancer} that provides configuration for multi-tenancy components.
 * <p>
 * This enhancer is the <b>single source of truth</b> for multi-tenancy wiring. It registers tenant-aware command bus
 * connector and the tenant provider. The distributed command bus enhancer then decorates the regular
 * {@link org.axonframework.messaging.commandhandling.CommandBus} with a {@link org.axonframework.messaging.commandhandling.distributed.DistributedCommandBus}
 * automatically once a {@link CommandBusConnector} is available.
 * <p>
 * <b>Default Segment Factories:</b> For embedded deployments without Axon Server, this enhancer still provides the
 * existing default implementations for event-side routing.
 * <ul>
 *   <li>{@link TenantEventSegmentFactory} - creates in-memory {@link EventStore} per tenant</li>
 * </ul>
 * These defaults can be overridden by registering custom implementations.
 * <p>
 * <b>Decoration Order:</b> Multi-tenant decorators run BEFORE intercepting decorators
 * (e.g., {@code InterceptingCommandBus}). This means the decoration chain is:
 * <pre>
 *     User → InterceptingCommandBus → DistributedCommandBus → Tenant-specific connectors
 * </pre>
 * This follows the standard Axon Framework pattern where interceptors wrap the outer bus,
 * and the multi-tenant bus handles routing to tenant-specific segments.
 * <p>
 * <b>Usage:</b> Users configure multi-tenancy via the {@link TenantResolverRegistry}:
 * <pre>{@code
 * var configurer = MessagingConfigurer.create();
 * configurer.componentRegistry(cr -> {
 *     cr.registerComponent(TenantProvider.class, config -> myProvider);
 *     cr.registerDecorator(TenantResolverRegistry.class, 0,
 *             (config, name, delegate) -> delegate.registerResolver(c -> myResolver));
 * });
 * }</pre>
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Theo Emanuelsson
 * @author Jan Galinski
 * @since 4.6.0
 */
public class MultiTenancyConfigurationDefaults implements ConfigurationEnhancer {

    /**
     * The order of {@code this} enhancer compared to others.
     * <p>
     * We need to register the multi-tenant connector before the distributed bus enhancer runs and before the Axon
     * Server connector enhancer installs its single-context connector.
     */
    public static final int ENHANCER_ORDER = Integer.MIN_VALUE + 5;

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }

    private final Consumer<ComponentRegistry> multiTenantCommandBusConnector = registerIfNotPresent(
            CommandBusConnector.class,
            config -> {
                TenantResolverRegistry registry = config.getComponent(TenantResolverRegistry.class);
                TenantResolver<Message> resolver = Objects.requireNonNull(
                        registry.commandResolver(config),
                        "No command resolver configured in TenantResolverRegistry"
                );
                MultiTenantAxonServerCommandBusConnector connector = new MultiTenantAxonServerCommandBusConnector(
                        config.getComponent(TenantProvider.class),
                        resolver,
                        config.getComponent(AxonServerConnectionManager.class),
                        config.getComponent(AxonServerConfiguration.class),
                        config.getComponent(MessageConverter.class)
                );
                registerTenantsIfProviderAvailable(config, connector);
                return connector;
            }
    );

    private final Consumer<ComponentRegistry> multiTenantQueryBusConnector = registerIfNotPresent(
            QueryBusConnector.class,
            config -> {
                TenantResolver<Message> resolver = Objects.requireNonNull(
                        config.getComponent(TenantResolverRegistry.class).queryResolver(config),
                        "No query resolver configured in TenantResolverRegistry"
                );
                MultiTenantAxonServerQueryBusConnector connector = new MultiTenantAxonServerQueryBusConnector(
                        config.getComponent(TenantProvider.class),
                        resolver,
                        config.getComponent(AxonServerConnectionManager.class),
                        config.getComponent(AxonServerConfiguration.class),
                        config.getComponent(MessageConverter.class)
                );
                registerTenantsIfProviderAvailable(config, connector);
                return connector;
            }
    );

    private final Consumer<ComponentRegistry> tenantResolverRegistry = registerIfNotPresent(
            TenantResolverRegistry.class,
            config -> new DefaultTenantResolverRegistry()
    );

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        List.of(
                registerIfNotPresent(TenantConnectPredicate.class, c -> TenantConnectPredicate.alwaysTrue()),
                registerIfNotPresent(axonServerTenantProvider(), SearchScope.ALL),
                registerIfNotPresent(TenantEventSegmentFactory.class,
                                      config -> tenant -> defaultEventStoreSegment(config, tenant)),

                tenantResolverRegistry,
                multiTenantCommandBusConnector,
                multiTenantQueryBusConnector,
                registerIfNotPresent(TenantPersistentStreamMessageSourceFactory.class, config ->
                        (name, persistentStreamProperties, scheduler, batchSize, context, configuration, tenantDescriptor) ->
                                new PersistentStreamMessageSource(
                                        name + "@" + tenantDescriptor.tenantId(),
                                        configuration.getComponent(AxonServerConnectionManager.class),
                                        configuration.getComponent(AxonServerConfiguration.class),
                                        configuration.getComponent(EventConverter.class),
                                        persistentStreamProperties,
                                        scheduler,
                                        configuration.getComponent(UnitOfWorkFactory.class),
                                        batchSize,
                                        StringUtils.emptyOrNull(context) ? tenantDescriptor.tenantId() : context
                                ))
        ).forEach(it -> it.accept(componentRegistry));

        // Register decorator to replace EventStore with TenantRoutingEventStore
        componentRegistry.registerDecorator(
                EventStore.class,
                TenantRoutingEventStore.DECORATION_ORDER,
                (config, name, delegate) -> createTenantRoutingEventStore(config, delegate)
        );

        // Register decorator to replace SnapshotStore with TenantRoutingSnapshotStore
        // FIXME
//        componentRegistry.registerDecorator(
//                SnapshotStore.class,
//                TenantRoutingSnapshotStore.DECORATION_ORDER,
//                (config, name, delegate) -> createTenantRoutingSnapshotStore(config, delegate)
//        );

        // Register tenant component parameter resolvers.
        // Discovers the tenant component registry at resolution time and registers it
        // for injection into message handlers.
        ParameterResolverFactoryUtils.registerToComponentRegistry(
                componentRegistry,
                this::createTenantComponentResolverFactory
        );

        componentRegistry.registerDecorator(
                CorrelationDataProviderRegistry.class,
                0,
                (config, name, delegate) -> delegate.registerProvider(cfg ->
                        new SimpleCorrelationDataProvider(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY))
        );
    }

    private EventStore createTenantRoutingEventStore(Configuration config, EventStore delegate) {
        TenantResolverRegistry resolverRegistry = config.getComponent(TenantResolverRegistry.class);
        if (!config.hasComponent(TenantEventSegmentFactory.class) || !resolverRegistry.hasResolver()) {
            return delegate;
        }

        TenantResolver<Message> resolver = resolverRegistry.resolver(config);
        if (resolver == null) {
            return delegate;
        }

        TenantEventSegmentFactory segmentFactory = config.getComponent(TenantEventSegmentFactory.class);
        TenantRoutingEventStore multiTenantStore = new TenantRoutingEventStore(segmentFactory, resolver);
        registerTenantsIfProviderAvailable(config, multiTenantStore);
        return multiTenantStore;
    }

    private ParameterResolverFactory createTenantComponentResolverFactory(Configuration config) {
        if (!config.hasComponent(TenantComponentRegistry.class)) {
            return (executable, parameters, parameterIndex) -> null;
        }

        TenantComponentRegistry<?> registry = config.getComponent(TenantComponentRegistry.class);
        return new ParameterResolverFactory() {
            @Override
            public @Nullable ParameterResolver<?> createInstance(Executable executable,
                                                                 Parameter[] parameters,
                                                                 int parameterIndex) {
                Class<?> parameterType = parameters[parameterIndex].getType();
                Class<?> componentType = registry.getComponentType();

                if (!parameterType.isAssignableFrom(componentType) && !componentType.isAssignableFrom(parameterType)) {
                    return null;
                }
                return new TenantComponentResolver(registry, parameterType, componentType);
            }
        };
    }

    private static final class TenantComponentResolver implements ParameterResolver<Object> {

        private final TenantComponentRegistry<?> registry;
        private final Class<?> parameterType;
        private final Class<?> componentType;

        private TenantComponentResolver(TenantComponentRegistry<?> registry,
                                        Class<?> parameterType,
                                        Class<?> componentType) {
            this.registry = registry;
            this.parameterType = parameterType;
            this.componentType = componentType;
        }

        @Override
        public CompletableFuture<Object> resolveParameterValue(ProcessingContext context) {
            Message message = Message.fromContext(context);
            if (message == null) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("No message found in ProcessingContext")
                );
            }

            String tenantId = message.metadata().get(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY);
            if (tenantId == null || tenantId.isBlank()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException(
                                "Missing tenant metadata [" + MetadataBasedTenantResolver.DEFAULT_TENANT_KEY
                                + "] on message [" + message.identifier() + "]"
                        )
                );
            }

            TenantDescriptor tenant = TenantDescriptor.tenantWithId(tenantId);
            Object component = parameterType.isAssignableFrom(componentType)
                    ? registry.getComponent(tenant)
                    : getTypedComponent(tenant, parameterType);
            return CompletableFuture.completedFuture(component);
        }

        @Override
        public boolean matches(ProcessingContext context) {
            Message message = Message.fromContext(context);
            return message != null
                   && message.metadata().get(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY) != null;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private Object getTypedComponent(TenantDescriptor tenant, Class<?> requestedType) {
            return ((TenantComponentRegistry) registry).getComponent(tenant, requestedType.asSubclass(componentType));
        }
    }

    private void registerTenantsIfProviderAvailable(Configuration config, MultiTenantAwareComponent component) {
        if (config.hasComponent(TenantProvider.class)) {
            TenantProvider tenantProvider = config.getComponent(TenantProvider.class);
            tenantProvider.subscribe(component);
            tenantProvider.getTenants().forEach(component::registerTenant);
        }
    }

    //FIXME
//    private SnapshotStore createTenantRoutingSnapshotStore(Configuration config, SnapshotStore delegate) {
//        TenantResolverRegistry resolverRegistry = config.getComponent(TenantResolverRegistry.class);
//        if (!config.hasComponent(TenantSnapshotStoreSegmentFactory.class) || !resolverRegistry.hasResolver()) {
//            return delegate;
//        }
//
//        // SnapshotStore resolves from the processing context's message (usually a command),
//        // not from events, so we use the general resolver.
//        TenantResolver<Message> resolver = resolverRegistry.resolver(config);
//        if (resolver == null) {
//            return delegate;
//        }
//
//        TenantSnapshotStoreSegmentFactory segmentFactory = config.getComponent(TenantSnapshotStoreSegmentFactory.class);
//        TenantRoutingSnapshotStore multiTenantStore = new TenantRoutingSnapshotStore(segmentFactory, resolver);
//
//        registerTenantsIfProviderAvailable(config, multiTenantStore);
//        return multiTenantStore;
//    }

    private EventStore defaultEventStoreSegment(Configuration config, TenantDescriptor tenant) {
        EventStore rawEventStore = config.getOptionalComponent(AxonServerConnectionManager.class)
                                         .map(ignored -> new StorageEngineBackedEventStore(
                                                 AxonServerEventStorageEngineFactory.constructForContext(
                                                         tenant.tenantId(),
                                                         config
                                                 ),
                                                 new SimpleEventBus(),
                                                 new AnnotationBasedTagResolver()
                                         ))
                                         .orElseGet(() -> new StorageEngineBackedEventStore(
                                                 new InMemoryEventStorageEngine(),
                                                 new SimpleEventBus(),
                                                 new AnnotationBasedTagResolver()
                                         ));

        List<MessageDispatchInterceptor<? super EventMessage>> dispatchInterceptors =
                config.getComponent(DispatchInterceptorRegistry.class).eventInterceptors(config,
                                                                                         EventStore.class,
                                                                                         null);

        return dispatchInterceptors.isEmpty()
                ? rawEventStore
                : new InterceptingEventStore(rawEventStore, dispatchInterceptors);
    }

    static ComponentDefinition<TenantProvider> axonServerTenantProvider() {
        return ComponentDefinition
                .ofType(TenantProvider.class)
                .withBuilder(config -> {
                    AxonServerConnectionManager connectionManager = config.getComponent(AxonServerConnectionManager.class);
                    TenantConnectPredicate connectPredicate = config.getComponent(TenantConnectPredicate.class,
                                                                                  TenantConnectPredicate::alwaysTrue);
                    return new AxonServerTenantProvider(
                            connectionManager,
                            connectPredicate
                    );
                })
                .onStart(Phase.INSTRUCTION_COMPONENTS + 10,
                         provider -> ((AxonServerTenantProvider) provider).start())
                .onShutdown(Phase.INSTRUCTION_COMPONENTS + 10,
                            provider -> ((AxonServerTenantProvider) provider).shutdown());
    }

    static <C> Consumer<ComponentRegistry> registerIfNotPresent(
            Class<C> type,
            ComponentBuilder<C> builder) {
        return cr -> cr.registerIfNotPresent(type, builder);
    }

    static <C> Consumer<ComponentRegistry> registerIfNotPresent(ComponentDefinition<C> definition,
                                                                SearchScope searchScope) {
        return cr -> cr.registerIfNotPresent(definition, searchScope);
    }
}
