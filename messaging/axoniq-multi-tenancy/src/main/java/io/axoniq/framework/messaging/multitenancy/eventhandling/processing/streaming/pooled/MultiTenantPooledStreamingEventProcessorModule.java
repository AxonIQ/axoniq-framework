/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.pooled;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.MultiTenantEventProcessor;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.MultiTenantEventProcessorModule;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.TenantEventProcessorSegmentFactory;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.token.store.TenantTokenStoreFactory;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.token.store.inmemory.InMemoryTenantTokenStoreFactory;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.transaction.TenantTransactionManagerFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantRoutingEventStore;
import org.axonframework.common.AxonThreadFactory;
import org.axonframework.common.configuration.BaseModule;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.common.configuration.ModuleBuilder;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.configuration.DefaultEventHandlingComponentsConfigurer;
import org.axonframework.messaging.eventhandling.configuration.EventHandlingComponentsConfigurer;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorCustomization;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.interception.InterceptingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SequenceCachingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;

//import org.axonframework.messaging.deadletter.SequencedDeadLetterQueue;
//import org.axonframework.messaging.eventhandling.deadletter.CachingSequencedDeadLetterQueue;
//import org.axonframework.messaging.eventhandling.deadletter.DeadLetteringEventHandlingComponent;

/**
 * A configuration module for configuring and registering a {@link MultiTenantEventProcessor} backed by per-tenant
 * {@link PooledStreamingEventProcessor} instances.
 * <p>
 * This module mirrors the fluent API of {@link PooledStreamingEventProcessorModule} but creates multi-tenant
 * infrastructure instead of a single processor. Each tenant gets its own {@link PooledStreamingEventProcessor}
 * with a dedicated event source (from {@link TenantRoutingEventStore#tenantSegments()}), token store (from
 * {@link TenantTokenStoreFactory}), and executor threads.
 * <p>
 * The main capabilities provided by this module include:
 * <ul>
 *     <li>Per-tenant pooled streaming event processors with isolated event sources and token stores</li>
 *     <li>Automatic thread pool configuration per tenant for coordinator and worker executors</li>
 *     <li>Event handling component decoration with interception and dead letter queues per tenant</li>
 *     <li>Integration with shared configuration customizations from parent modules</li>
 *     <li>Lifecycle management for the {@link MultiTenantEventProcessor} and its tenant segments</li>
 *     <li>Dynamic tenant registration via {@link TenantProvider}</li>
 * </ul>
 * <p>
 * This module is typically created through
 * {@link MultiTenantEventProcessorModule#pooledStreaming(String)}.
 * <p>
 * Example usage:
 * <pre>{@code
 * var module = MultiTenantEventProcessorModule
 *     .pooledStreaming("order-processor")
 *     .eventHandlingComponents(c -> c.component("orders", cfg -> orderHandler))
 *     .customized((cfg, config) -> config.initialSegmentCount(4));
 * }</pre>
 *
 * @author Jan Galinski
 * @author Theo Emanuelsson
 * @since 5.3.0
 * @see MultiTenantEventProcessor
 * @see PooledStreamingEventProcessorModule
 */
public class MultiTenantPooledStreamingEventProcessorModule
        extends BaseModule<MultiTenantPooledStreamingEventProcessorModule>
        implements EventProcessorModule, ModuleBuilder<MultiTenantPooledStreamingEventProcessorModule>,
        EventProcessorModule.EventHandlingPhase<MultiTenantPooledStreamingEventProcessorModule, PooledStreamingEventProcessorConfiguration>,
        EventProcessorModule.CustomizationPhase<MultiTenantPooledStreamingEventProcessorModule, PooledStreamingEventProcessorConfiguration> {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenantPooledStreamingEventProcessorModule.class);

    private final String processorName;
    private Map<String, ComponentBuilder<EventHandlingComponent>> eventHandlingComponentBuilders;
    private ComponentBuilder<PooledStreamingEventProcessorConfiguration> customizedProcessorConfigurationBuilder;

    /**
     * Constructs a module with the given processor name.
     * <p>
     * The processor name will be used as the module name and as the unique identifier for the
     * {@link MultiTenantEventProcessor} component created by this module.
     *
     * @param processorName the unique name for the multi-tenant event processor
     */
    public MultiTenantPooledStreamingEventProcessorModule(String processorName) {
        super(processorName);
        this.processorName = processorName;
    }

    @Override
    public MultiTenantPooledStreamingEventProcessorModule build() {
        // Infrastructure creation is deferred to build(Configuration, LifecycleRegistry)
        // where the parent configuration is available.
        return this;
    }

    @Override
    public Configuration build(Configuration parent, LifecycleRegistry lifecycleRegistry) {
        componentRegistry(cr -> createMultiTenantInfrastructure(cr, parent));
        return super.build(parent, lifecycleRegistry);
    }

    @Override
    public MultiTenantPooledStreamingEventProcessorModule customized(
            BiFunction<Configuration, PooledStreamingEventProcessorConfiguration, PooledStreamingEventProcessorConfiguration> instanceCustomization
    ) {
        this.customizedProcessorConfigurationBuilder = config -> {
            var typeCustomization = typeSpecificCustomizationOrNoOp(config)
                    .apply(config, defaultEventProcessorsConfiguration(config, processorName));
            return instanceCustomization.apply(config, typeCustomization);
        };
        return this;
    }

    @Override
    public MultiTenantPooledStreamingEventProcessorModule notCustomized() {
        if (customizedProcessorConfigurationBuilder == null) {
            customized((cfg, config) -> config);
        }
        return this;
    }

    @Override
    public CustomizationPhase<MultiTenantPooledStreamingEventProcessorModule, PooledStreamingEventProcessorConfiguration> eventHandlingComponents(
            Function<EventHandlingComponentsConfigurer.RequiredComponentPhase, EventHandlingComponentsConfigurer.CompletePhase> configurerTask
    ) {
        Objects.requireNonNull(configurerTask, "configurerTask may not be null");
        var componentsConfigurer = new DefaultEventHandlingComponentsConfigurer();
        this.eventHandlingComponentBuilders = configurerTask.apply(componentsConfigurer).toMap();
        return this;
    }

    // ---- Customization chain helpers (mirrors PooledStreamingEventProcessorModule) ----

    private static PooledStreamingEventProcessorModule.Customization typeSpecificCustomizationOrNoOp(
            Configuration cfg
    ) {
        return cfg.getOptionalComponent(PooledStreamingEventProcessorModule.Customization.class)
                  .orElseGet(PooledStreamingEventProcessorModule.Customization::noOp);
    }

    private static PooledStreamingEventProcessorConfiguration defaultEventProcessorsConfiguration(
            Configuration config,
            String processorName
    ) {
        return new PooledStreamingEventProcessorConfiguration(
                parentSharedCustomizationOrDefault(config)
                        .apply(config, new EventProcessorConfiguration(processorName, config)),
                config
        );
    }

    private static EventProcessorCustomization parentSharedCustomizationOrDefault(
            Configuration cfg
    ) {
        return cfg.getOptionalComponent(EventProcessorCustomization.class)
                  .orElseGet(EventProcessorCustomization::noOp);
    }

    // ---- Multi-tenant infrastructure creation ----

    private void createMultiTenantInfrastructure(ComponentRegistry registry, Configuration parent) {
        registerCustomizedConfiguration(registry);
        registerMultiTenantEventProcessor(registry, parent);
    }

    private void registerCustomizedConfiguration(ComponentRegistry registry) {
        registry.registerComponent(
                ComponentDefinition
                        .ofType(PooledStreamingEventProcessorConfiguration.class)
                        .withBuilder(cfg -> {
                            var configuration = customizedProcessorConfigurationBuilder.build(cfg);
                            // Do NOT default executor creation here; each tenant creates its own executors.
                            return configuration;
                        })
        );
    }

    private void registerMultiTenantEventProcessor(ComponentRegistry registry, Configuration parent) {
        var processorComponentDefinition = ComponentDefinition
                .ofTypeAndName(StreamingEventProcessor.class, processorName)
                .withBuilder(cfg -> {
                    var tenantTokenStoreFactory = cfg.getOptionalComponent(TenantTokenStoreFactory.class)
                                                    .orElseGet(InMemoryTenantTokenStoreFactory::new); // TODO: needed?
                    var tenantTransactionManagerFactory = cfg.getOptionalComponent(TenantTransactionManagerFactory.class);
                    TenantEventProcessorSegmentFactory tenantSegmentFactory = tenantDescriptor ->
                            createTenantProcessor(tenantDescriptor, cfg, tenantTokenStoreFactory,
                                                  tenantTransactionManagerFactory);
                    var processor = new MultiTenantEventProcessor(processorName, tenantSegmentFactory);
                    // Register initial tenants if a TenantProvider is available
                    cfg.getOptionalComponent(TenantProvider.class).ifPresent(tenantProvider -> {
                        tenantProvider.subscribe(processor);
                        tenantProvider.getTenants().forEach(processor::registerTenant);
                    });
                    return processor;
                })
                .onStart(Phase.INBOUND_EVENT_CONNECTORS, (cfg, component) -> {
                    return component.start();
                })
                .onShutdown(Phase.INBOUND_EVENT_CONNECTORS, (cfg, component) -> {
                    return component.shutdown();
                });

        registry.registerComponent(processorComponentDefinition);
    }

    @SuppressWarnings("unchecked")
    private StreamingEventProcessor createTenantProcessor(
            TenantDescriptor tenantDescriptor,
            Configuration parent,
            TenantTokenStoreFactory tenantTokenStoreFactory,
            Optional<TenantTransactionManagerFactory> tenantTransactionManagerFactory
    ) {
        var processorConfig = customizedProcessorConfigurationBuilder.build(parent);
        String tenantProcessorName = processorName + "[" + tenantDescriptor.tenantId() + "]";

        // Per-tenant executors
        var workerExecutor = Optional.ofNullable(processorConfig.workerExecutor())
                                     .orElseGet(() -> defaultExecutor(4, "WorkPackage[" + tenantProcessorName + "]"));
        var coordinatorExecutor = Optional.ofNullable(processorConfig.coordinatorExecutor())
                                          .orElseGet(() -> defaultExecutor(1, "Coordinator[" + tenantProcessorName + "]"));
        processorConfig.workerExecutor(workerExecutor);
        processorConfig.coordinatorExecutor(coordinatorExecutor);

        // Per-tenant token store
        TokenStore tenantTokenStore = tenantTokenStoreFactory.apply(tenantDescriptor);
        processorConfig.tokenStore(tenantTokenStore);

        tenantTransactionManagerFactory.ifPresent(factory -> processorConfig.unitOfWorkFactory(
                new TransactionalUnitOfWorkFactory(
                        factory.apply(tenantDescriptor),
                        processorConfig.unitOfWorkFactory()
                )
        ));

        // Per-tenant event source from the TenantRoutingEventStore, even when wrapped by decorators.
        processorConfig.eventSource(resolveTenantEventSource(parent, tenantDescriptor).orElseThrow(() ->
                new IllegalStateException(
                        "Could not resolve tenant event source for [" + tenantDescriptor.tenantId()
                        + "] from the configured EventStore chain."
                )));

        // Build per-tenant event handling components
        List<EventHandlingComponent> tenantComponents = eventHandlingComponentBuilders.entrySet().stream()
                .map(entry -> buildTenantEventHandlingComponent(
                        entry.getKey(), entry.getValue(), parent, processorConfig, tenantProcessorName
                ))
                .toList();

        return new PooledStreamingEventProcessor(tenantProcessorName, tenantComponents, processorConfig);
    }

    @SuppressWarnings("unchecked")
    private EventHandlingComponent buildTenantEventHandlingComponent(
            String componentName,
            ComponentBuilder<EventHandlingComponent> componentBuilder,
            Configuration parent,
            PooledStreamingEventProcessorConfiguration processorConfig,
            String tenantProcessorName
    ) {
        // Build the base component
        EventHandlingComponent component = componentBuilder.build(parent);

        // Wrap with sequence caching
        component = new SequenceCachingEventHandlingComponent(component);

        // Wrap with interceptors
        component = new InterceptingEventHandlingComponent(
                processorConfig.interceptors(),
                component
        );

//   FIXME: DLQ     // Wrap with dead letter queue if enabled
//        var dlqConfig = processorConfig.deadLetterQueue();
//        if (dlqConfig.isEnabled()) {
//            var dlqName = "DeadLetterQueue[" + tenantProcessorName + "][" + componentName + "]";
//            SequencedDeadLetterQueue<EventMessage> dlq = dlqConfig.factory().create(dlqName, null);
//            if (dlqConfig.cacheMaxSize() > 0) {
//                dlq = new CachingSequencedDeadLetterQueue<>(dlq, dlqConfig.cacheMaxSize());
//            }
//            component = new DeadLetteringEventHandlingComponent(
//                    component,
//                    dlq,
//                    dlqConfig.enqueuePolicy(),
//                    processorConfig.unitOfWorkFactory(),
//                    dlqConfig.clearOnReset()
//            );
//        }

        return component;
    }

    private static ScheduledExecutorService defaultExecutor(int poolSize, String factoryName) {
        return Executors.newScheduledThreadPool(poolSize, new AxonThreadFactory(factoryName));
    }

    private Optional<EventStore> resolveTenantEventSource(Configuration parent, TenantDescriptor tenantDescriptor) {
        return parent.getOptionalComponent(EventStore.class)
                     .flatMap(MultiTenantPooledStreamingEventProcessorModule::resolveTenantRoutingEventStore)
                     .map(TenantRoutingEventStore::tenantSegments)
                     .map(segments -> segments.get(tenantDescriptor));
    }

    static Optional<TenantRoutingEventStore> resolveTenantRoutingEventStore(Object candidate) {
        return resolveTenantRoutingEventStore(candidate, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static Optional<TenantRoutingEventStore> resolveTenantRoutingEventStore(
            Object candidate,
            Set<Object> visited
    ) {
        if (candidate == null || !visited.add(candidate)) {
            return Optional.empty();
        }
        if (candidate instanceof TenantRoutingEventStore tenantRoutingEventStore) {
            return Optional.of(tenantRoutingEventStore);
        }

        Class<?> currentType = candidate.getClass();
        while (currentType != null && currentType != Object.class) {
            for (Field field : currentType.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                try {
                    Object value = field.get(candidate);
                    if (value instanceof EventStore nestedEventStore) {
                        Optional<TenantRoutingEventStore> nested =
                                resolveTenantRoutingEventStore(nestedEventStore, visited);
                        if (nested.isPresent()) {
                            return nested;
                        }
                    }
                } catch (IllegalAccessException ignored) {
                    // Best-effort unwrapping. If the field is inaccessible, continue scanning the chain.
                }
            }
            currentType = currentType.getSuperclass();
        }

        return Optional.empty();
    }

}
