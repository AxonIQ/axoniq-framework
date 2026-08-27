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

package io.axoniq.framework.axonserver.connector.configuration;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.api.TagsConfiguration;
import io.axoniq.framework.axonserver.connector.command.AxonServerCommandBusConnector;
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngineFactory;
import io.axoniq.framework.axonserver.connector.event.EventProcessorControlService;
import io.axoniq.framework.axonserver.connector.query.AxonServerQueryBusConnector;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.commandhandling.distributed.PayloadConvertingCommandBusConnector;
import io.axoniq.framework.messaging.queryhandling.distributed.PayloadConvertingQueryBusConnector;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentDecorator;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentLifecycleHandler;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link ConfigurationEnhancer} that is auto-loadable by the
 * {@link ApplicationConfigurer}, setting sensible defaults when using Axon Server.
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 * @since 4.0.0
 */
public class AxonServerConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * The {@link #order()} when this {@link AxonServerConfigurationEnhancer} enhances an
     * {@link ApplicationConfigurer}.
     */
    public static final int ENHANCER_ORDER = Integer.MIN_VALUE + 10;

    @Override
    public void enhance(ComponentRegistry registry) {
        ComponentBuilder<AxonServerEventStorageEngine> sharedStorageEngineBuilder = eventStorageEngineBuilder();

        registry.registerIfNotPresent(AxonServerConfiguration.class,
                                      c -> new AxonServerConfiguration(),
                                      SearchScope.ALL)
                .registerIfNotPresent(connectionManagerDefinition(), SearchScope.ALL)
                .registerIfNotPresent(ManagedChannelCustomizer.class,
                                      c -> ManagedChannelCustomizer.identity(),
                                      SearchScope.ALL)
                .registerIfNotPresent(EventStorageEngine.class, sharedStorageEngineBuilder, SearchScope.ALL)
                .registerIfNotPresent(SnapshotStore.class, sharedStorageEngineBuilder, SearchScope.ALL)
                .registerIfNotPresent(commandBusConnectorDefinition(), SearchScope.ALL)
                .registerIfNotPresent(queryBusConnectorDefinition(), SearchScope.ALL)
                .registerDecorator(CommandBusConnector.class,
                                   0,
                                   payloadConvertingConnectorComponentDecorator()
                )
                .registerDecorator(QueryBusConnector.class,
                                   0,
                                   payloadConvertingQueryBusConnectorComponentDecorator()
                )
                .registerDecorator(topologyChangeListenerRegistration())
                .registerFactory(new AxonServerEventStorageEngineFactory())
                .registerIfNotPresent(eventProcessorControlService());
    }

    private static ComponentDefinition<AxonServerConnectionManager> connectionManagerDefinition() {
        return ComponentDefinition.ofType(AxonServerConnectionManager.class)
                                  .withBuilder(AxonServerConfigurationEnhancer::buildConnectionManager)
                                  .onStart(Phase.INSTRUCTION_COMPONENTS, AxonServerConnectionManager::start)
                                  .onShutdown(Phase.EXTERNAL_CONNECTIONS, AxonServerConnectionManager::shutdown);
    }

    private static AxonServerConnectionManager buildConnectionManager(Configuration config) {
        AxonServerConfiguration serverConfig = config.getComponent(AxonServerConfiguration.class);
        return AxonServerConnectionManager.builder()
                                          .routingServers(serverConfig.getServers())
                                          .axonServerConfiguration(serverConfig)
                                          .tagsConfiguration(
                                                  config.getComponent(TagsConfiguration.class, TagsConfiguration::new)
                                          )
                                          .channelCustomizer(config.getComponent(ManagedChannelCustomizer.class))
                                          .build();
    }

    private static ComponentBuilder<AxonServerEventStorageEngine> eventStorageEngineBuilder() {
        AtomicReference<@Nullable AxonServerEventStorageEngine> instance = new AtomicReference<>();
        return config -> {
            AxonServerEventStorageEngine result = instance.updateAndGet(
                    e -> {
                        if (e != null) {
                            return e;
                        } else {
                            String defaultContext = config.getComponent(AxonServerConfiguration.class).getContext();
                            return AxonServerEventStorageEngineFactory.constructForContext(defaultContext, config);
                        }
                    }
            );
            return Objects.requireNonNull(result, "AxonServerEventStorageEngine must not be null");
        };
    }

    private static ComponentDefinition<CommandBusConnector> commandBusConnectorDefinition() {
        return ComponentDefinition.ofType(CommandBusConnector.class)
                                  .withBuilder(config -> new AxonServerCommandBusConnector(
                                          config.getComponent(AxonServerConnectionManager.class).getConnection(),
                                          config.getComponent(AxonServerConfiguration.class),
                                          config.getComponent(MessageConverter.class)
                                  ))
                                  .onStart(Phase.INBOUND_COMMAND_CONNECTOR,
                                           connector -> ((AxonServerCommandBusConnector) connector).start())
                                  .onShutdown(Phase.INBOUND_COMMAND_CONNECTOR,
                                              (ComponentLifecycleHandler<CommandBusConnector>) (config, connector) ->
                                                      ((AxonServerCommandBusConnector) connector).disconnect())
                                  .onShutdown(Phase.OUTBOUND_COMMAND_CONNECTORS,
                                              (ComponentLifecycleHandler<CommandBusConnector>) (config, connector) ->
                                                      ((AxonServerCommandBusConnector) connector).shutdownDispatching());
    }

    private static ComponentDefinition<QueryBusConnector> queryBusConnectorDefinition() {
        return ComponentDefinition.ofType(QueryBusConnector.class)
                                  .withBuilder(config -> new AxonServerQueryBusConnector(
                                          config.getComponent(AxonServerConnectionManager.class).getConnection(),
                                          config.getComponent(AxonServerConfiguration.class),
                                          config.getComponent(MessageConverter.class)
                                  ))
                                  .onStart(Phase.INBOUND_QUERY_CONNECTOR,
                                           connector -> ((AxonServerQueryBusConnector) connector).start())
                                  .onShutdown(Phase.INBOUND_QUERY_CONNECTOR,
                                              (ComponentLifecycleHandler<QueryBusConnector>) (config, connector) ->
                                                      ((AxonServerQueryBusConnector) connector).disconnect())
                                  .onShutdown(Phase.OUTBOUND_QUERY_CONNECTORS,
                                              (ComponentLifecycleHandler<QueryBusConnector>) (config, connector) ->
                                                      ((AxonServerQueryBusConnector) connector).shutdownDispatching());
    }

    private static ComponentDecorator<QueryBusConnector, PayloadConvertingQueryBusConnector> payloadConvertingQueryBusConnectorComponentDecorator() {
        return (config, name, delegate) -> new PayloadConvertingQueryBusConnector(
                delegate,
                config.getComponent(MessageConverter.class),
                byte[].class
        );
    }

    private static ComponentDecorator<CommandBusConnector, PayloadConvertingCommandBusConnector> payloadConvertingConnectorComponentDecorator() {
        return (config, name, delegate) -> new PayloadConvertingCommandBusConnector(
                delegate,
                config.getComponent(MessageConverter.class),
                byte[].class
        );
    }

    private static DecoratorDefinition<AxonServerConnectionManager, AxonServerConnectionManager> topologyChangeListenerRegistration() {
        return DecoratorDefinition.forType(AxonServerConnectionManager.class)
                                  .with((config, name, delegate) -> delegate)
                                  .onStart(Phase.INSTRUCTION_COMPONENTS, (config, connectionManager) -> {
                                      Optional<TopologyChangeListener> topologyChangeListener =
                                              config.getOptionalComponent(TopologyChangeListener.class);
                                      topologyChangeListener.ifPresent(
                                              changeListener -> connectionManager.getConnection()
                                                                                 .controlChannel()
                                                                                 .registerTopologyChangeHandler(
                                                                                         changeListener)
                                      );
                                      return FutureUtils.emptyCompletedFuture();
                                  });
    }

    private static ComponentDefinition<EventProcessorControlService> eventProcessorControlService() {
        return ComponentDefinition.ofType(EventProcessorControlService.class)
                                  .withBuilder(c -> {
                                      AxonServerConfiguration serverConfig =
                                              c.getComponent(AxonServerConfiguration.class);
                                      return new EventProcessorControlService(
                                              c, c.getComponent(AxonServerConnectionManager.class),
                                              serverConfig.getContext(),
                                              serverConfig.getEventhandling().getProcessors()
                                      );
                                  })
                                  .onStart(Phase.INSTRUCTION_COMPONENTS, EventProcessorControlService::start);
    }

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }
}
