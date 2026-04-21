/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.platform.framework;

import io.axoniq.platform.framework.application.ApplicationMetricRegistry;
import io.axoniq.platform.framework.application.ApplicationMetricReporter;
import io.axoniq.platform.framework.application.ApplicationReportCreator;
import io.axoniq.platform.framework.application.ApplicationThreadDumpProvider;
import io.axoniq.platform.framework.application.RSocketThreadDumpResponder;
import io.axoniq.platform.framework.client.AxoniqConsoleRSocketClient;
import io.axoniq.platform.framework.client.PlatformClientConnectionService;
import io.axoniq.platform.framework.client.RSocketHandlerRegistrar;
import io.axoniq.platform.framework.client.ServerProcessorReporter;
import io.axoniq.platform.framework.client.SetupPayloadCreator;
import io.axoniq.platform.framework.client.strategy.CborJackson3EncodingStrategy;
import io.axoniq.platform.framework.client.strategy.RSocketPayloadEncodingStrategy;
import io.axoniq.platform.framework.eventprocessor.AxoniqPlatformEventHandlingComponent;
import io.axoniq.platform.framework.eventprocessor.EventProcessorManager;
import io.axoniq.platform.framework.eventprocessor.ProcessorMetricsRegistry;
import io.axoniq.platform.framework.eventprocessor.ProcessorReportCreator;
import io.axoniq.platform.framework.eventprocessor.RSocketProcessorResponder;
import io.axoniq.platform.framework.messaging.AxoniqPlatformCommandBus;
import io.axoniq.platform.framework.messaging.AxoniqPlatformQueryBus;
import io.axoniq.platform.framework.messaging.HandlerMetricsRegistry;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.common.configuration.Module;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorModule;
import org.axonframework.messaging.queryhandling.QueryBus;

import java.util.UUID;

public class AxoniqPlatformConfigurerEnhancer implements ConfigurationEnhancer {

    public static final int PLATFORM_ENHANCER_ORDER = Integer.MAX_VALUE - 5;

    @Override
    public void enhance(ComponentRegistry registry) {
        if (!registry.hasComponent(AxoniqPlatformConfiguration.class)
                || registry.hasComponent(AxoniqConsoleRSocketClient.class)) {
            return;
        }
        registry
                .registerComponent(ComponentDefinition
                                           .ofType(PlatformClientConnectionService.class)
                                           .withBuilder(c -> new PlatformClientConnectionService()))
                .registerComponent(ComponentDefinition
                                           .ofType(ProcessorMetricsRegistry.class)
                                           .withBuilder(c -> new ProcessorMetricsRegistry()))
                .registerComponent(ComponentDefinition
                                           .ofType(ApplicationMetricRegistry.class)
                                           .withBuilder(c -> new ApplicationMetricRegistry()))
                .registerComponent(ComponentDefinition
                                           .ofType(ProcessorReportCreator.class)
                                           .withBuilder(ProcessorReportCreator::new))
                .registerComponent(ComponentDefinition
                                           .ofType(ApplicationReportCreator.class)
                                           .withBuilder(c -> new ApplicationReportCreator(
                                                   c.getComponent(ApplicationMetricRegistry.class)
                                           )))
                .registerComponent(ComponentDefinition
                                           .ofType(SetupPayloadCreator.class)
                                           .withBuilder(SetupPayloadCreator::new))
                .registerComponent(ComponentDefinition
                                           .ofType(EventProcessorManager.class)
                                           .withBuilder(EventProcessorManager::new))
                .registerComponent(ComponentDefinition
                                           .ofType(RSocketPayloadEncodingStrategy.class)
                                           .withBuilder(c -> createEncodingStrategy()))
                .registerComponent(ComponentDefinition
                                           .ofType(RSocketHandlerRegistrar.class)
                                           .withBuilder(c -> new RSocketHandlerRegistrar(c.getComponent(
                                                   RSocketPayloadEncodingStrategy.class))))
                .registerComponent(ComponentDefinition
                                           .ofType(RSocketProcessorResponder.class)
                                           .withBuilder(c -> new RSocketProcessorResponder(
                                                   c.getComponent(EventProcessorManager.class),
                                                   c.getComponent(ProcessorReportCreator.class),
                                                   c.getComponent(RSocketHandlerRegistrar.class)))
                                           .onStart(Phase.EXTERNAL_CONNECTIONS, RSocketProcessorResponder::start))
                .registerComponent(ComponentDefinition
                                           .ofType(AxoniqConsoleRSocketClient.class)
                                           .withBuilder(c -> new AxoniqConsoleRSocketClient(
                                                   c.getComponent(AxoniqPlatformConfiguration.class),
                                                   c.getComponent(SetupPayloadCreator.class),
                                                   c.getComponent(RSocketHandlerRegistrar.class),
                                                   c.getComponent(RSocketPayloadEncodingStrategy.class),
                                                   c.getComponent(PlatformClientConnectionService.class),
                                                   determineInstanceName(c)))

                                           .onStart(Phase.EXTERNAL_CONNECTIONS, AxoniqConsoleRSocketClient::start))
                .registerComponent(ComponentDefinition
                                           .ofType(ServerProcessorReporter.class)
                                           .withBuilder(c -> new ServerProcessorReporter(
                                                   c.getComponent(AxoniqConsoleRSocketClient.class),
                                                   c.getComponent(ProcessorReportCreator.class),
                                                   c.getComponent(PlatformClientConnectionService.class),
                                                   c.getComponent(AxoniqPlatformConfiguration.class)))
                                           // The start handler will allow for eager creation
                                           .onStart(Phase.EXTERNAL_CONNECTIONS, c -> {
                                           }))
                .registerComponent(ComponentDefinition
                                           .ofType(ApplicationMetricReporter.class)
                                           .withBuilder(c -> new ApplicationMetricReporter(
                                                   c.getComponent(AxoniqConsoleRSocketClient.class),
                                                   c.getComponent(ApplicationReportCreator.class),
                                                   c.getComponent(PlatformClientConnectionService.class),
                                                   c.getComponent(AxoniqPlatformConfiguration.class)))
                                           // The start handler will allow for eager creation
                                           .onStart(Phase.EXTERNAL_CONNECTIONS, c -> {
                                           }))
                .registerComponent(ComponentDefinition
                                           .ofType(HandlerMetricsRegistry.class)
                                           .withBuilder(c -> new HandlerMetricsRegistry(
                                                   c.getComponent(AxoniqConsoleRSocketClient.class),
                                                   c.getComponent(PlatformClientConnectionService.class),
                                                   c.getComponent(AxoniqPlatformConfiguration.class))))
                .registerComponent(ComponentDefinition
                                           .ofType(ApplicationThreadDumpProvider.class)
                                           .withBuilder(c -> new ApplicationThreadDumpProvider()))
                .registerDecorator(DecoratorDefinition.forType(CommandBus.class)
                                                      .with((c, name, delegate) -> {
                                                          return new AxoniqPlatformCommandBus(
                                                                  delegate,
                                                                  c.getComponent(HandlerMetricsRegistry.class)
                                                          );
                                                      })
                                                      .order(AxoniqPlatformCommandBus.DECORATION_ORDER))
                .registerDecorator(DecoratorDefinition.forType(QueryBus.class)
                                                      .with((c, name, delegate) -> {
                                                          return new AxoniqPlatformQueryBus(
                                                                  delegate,
                                                                  c.getComponent(HandlerMetricsRegistry.class)
                                                          );
                                                      })
                                                      .order(AxoniqPlatformQueryBus.DECORATION_ORDER))
                .registerComponent(ComponentDefinition
                                           .ofType(RSocketThreadDumpResponder.class)
                                           .withBuilder(c -> new RSocketThreadDumpResponder(
                                                   c.getComponent(ApplicationThreadDumpProvider.class),
                                                   c.getComponent(RSocketHandlerRegistrar.class)))
                                           // The start handler will allow for eager creation
                                           .onStart(Phase.EXTERNAL_CONNECTIONS, RSocketThreadDumpResponder::start));


        UtilsKt.doOnSubModules(registry, (componentRegistry, module) -> {
            componentRegistry
                    .registerDecorator(DecoratorDefinition.forType(EventHandlingComponent.class)
                                                          .with((cc, name, delegate) ->
                                                                        new AxoniqPlatformEventHandlingComponent(
                                                                                delegate,
                                                                                getProcessorName(module),
                                                                                cc.getComponent(HandlerMetricsRegistry.class),
                                                                                cc.getComponent(ProcessorMetricsRegistry.class)))
                                                          .order(0));

            return null;
        });
    }

    /**
     * Checks the classpath for Jackson 3 and its requirements for this application.
     * Will fail to create the component if Jackson 3 is not present or doesn't have the right modules.
     */
    private static RSocketPayloadEncodingStrategy createEncodingStrategy() {
        try {
            Class.forName("tools.jackson.databind.ObjectMapper");
            try {
                Class.forName("tools.jackson.dataformat.cbor.CBORMapper");
                try {
                    Class.forName("tools.jackson.module.kotlin.KotlinModule");
                    return new CborJackson3EncodingStrategy();
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException(
                            "Found Jackson 3 on the classpath, but can not find the KotlinModule. Please add the tools.jackson.module:jackson-module-kotlin dependency to your project");
                }
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(
                        "Found Jackson 3 on the classpath, but cannot find the CBOR dataformat. Please add the tools.jackson.dataformat:jackson-dataformat-cbor dependency to your project.");
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "Jackson 3 was not found on the classpath. Please add Jackson 3 to your project.");
        }
    }

    /**
     * Determines the instance name. Either takes the exact instanceId that Axon Server connector has generated to
     * correlate the two as much as possible, or falls back to a random generated 4-byte string..
     */
    private String determineInstanceName(Configuration c) {
        try {
            Class<?> connectionManagerClazz = Class.forName(
                    "io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager");
            if (c.hasComponent(connectionManagerClazz)) {
                Object connectionManager = c.getComponent(connectionManagerClazz);
                // Get private connectionFactory field of type factoryClazz
                Object connectionFactory = ReflectionKt.getPropertyValue(connectionManager, "connectionFactory");
                if (connectionFactory != null) {
                    // Now call getClientInstanceId public method
                    String instanceId = ReflectionKt.getPropertyValue(connectionFactory, "clientInstanceId");

                    if (instanceId != null && instanceId.matches(".*-[0-9a-fA-F]{8}$")) {
                        return instanceId;
                    }
                }
            }
        } catch (ClassNotFoundException e) {
            // Do nothing
        }
        // No AS, or not AS client 2026+. Fall back to random nodeId.
        return c.getComponent(AxoniqPlatformConfiguration.class).getHostname() + "-" + randomNodeId();
    }


    private String randomNodeId() {
        String uuid = UUID.randomUUID().toString();
        return uuid.substring(uuid.length() - 4);
    }

    @Override
    public int order() {
        return PLATFORM_ENHANCER_ORDER;
    }

    private static String getProcessorName(Module module) {
        String processorName = null;
        if (module instanceof PooledStreamingEventProcessorModule) {
            processorName = ReflectionKt.getPropertyValue(module, "processorName");
        } else if (module instanceof SubscribingEventProcessorModule) {
            processorName = ReflectionKt.getPropertyValue(module, "processorName");
        }
        return processorName;
    }
}
