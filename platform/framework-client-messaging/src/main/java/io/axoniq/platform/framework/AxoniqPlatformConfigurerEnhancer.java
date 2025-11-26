/*
 * Copyright (c) 2022-2025. AxonIQ B.V.
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

package io.axoniq.platform.framework;

import io.axoniq.platform.framework.application.ApplicationMetricRegistry;
import io.axoniq.platform.framework.application.ApplicationMetricReporter;
import io.axoniq.platform.framework.application.ApplicationReportCreator;
import io.axoniq.platform.framework.application.ApplicationThreadDumpProvider;
import io.axoniq.platform.framework.application.BusType;
import io.axoniq.platform.framework.application.MeasuringExecutorServiceDecorator;
import io.axoniq.platform.framework.application.RSocketThreadDumpResponder;
import io.axoniq.platform.framework.client.AxoniqConsoleRSocketClient;
import io.axoniq.platform.framework.client.ClientSettingsService;
import io.axoniq.platform.framework.client.RSocketHandlerRegistrar;
import io.axoniq.platform.framework.client.ServerProcessorReporter;
import io.axoniq.platform.framework.client.SetupPayloadCreator;
import io.axoniq.platform.framework.client.strategy.CborEncodingStrategy;
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
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.common.configuration.Module;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.distributed.DistributedCommandBusConfiguration;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorModule;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.distributed.DistributedQueryBusConfiguration;

public class AxoniqPlatformConfigurerEnhancer implements ConfigurationEnhancer {

    public static final int PLATFORM_ENHANCER_ORDER = Integer.MAX_VALUE - 5;

    @Override
    public void enhance(ComponentRegistry registry) {
        if (!registry.hasComponent(AxoniqPlatformConfiguration.class)) {
            return;
        }
        registry
                .registerComponent(ComponentDefinition
                                           .ofType(ClientSettingsService.class)
                                           .withBuilder(c -> new ClientSettingsService()))
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
                                           .withBuilder(c -> new CborEncodingStrategy()))
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
                                                   c.getComponent(ClientSettingsService.class)))

                                           .onStart(Phase.EXTERNAL_CONNECTIONS, AxoniqConsoleRSocketClient::start))
                .registerComponent(ComponentDefinition
                                           .ofType(ServerProcessorReporter.class)
                                           .withBuilder(c -> new ServerProcessorReporter(
                                                   c.getComponent(AxoniqConsoleRSocketClient.class),
                                                   c.getComponent(ProcessorReportCreator.class),
                                                   c.getComponent(ClientSettingsService.class),
                                                   c.getComponent(AxoniqPlatformConfiguration.class)))
                                           // The start handler will allow for eager creation
                                           .onStart(Phase.EXTERNAL_CONNECTIONS, c -> {
                                           }))
                .registerComponent(ComponentDefinition
                                           .ofType(ApplicationMetricReporter.class)
                                           .withBuilder(c -> new ApplicationMetricReporter(
                                                   c.getComponent(AxoniqConsoleRSocketClient.class),
                                                   c.getComponent(ApplicationReportCreator.class),
                                                   c.getComponent(ClientSettingsService.class),
                                                   c.getComponent(AxoniqPlatformConfiguration.class)))
                                           // The start handler will allow for eager creation
                                           .onStart(Phase.EXTERNAL_CONNECTIONS, c -> {
                                           }))
                .registerComponent(ComponentDefinition
                                           .ofType(HandlerMetricsRegistry.class)
                                           .withBuilder(c -> new HandlerMetricsRegistry(
                                                   c.getComponent(AxoniqConsoleRSocketClient.class),
                                                   c.getComponent(ClientSettingsService.class),
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
                                           .onStart(Phase.EXTERNAL_CONNECTIONS, RSocketThreadDumpResponder::start))
                .registerDecorator(DecoratorDefinition.forType(DistributedCommandBusConfiguration.class)
                                                      .with((cc, name, delegate) -> {
                                                          return new DistributedCommandBusConfiguration(
                                                                  delegate.loadFactor(),
                                                                  delegate.commandThreads(),
                                                                  (config, queue) -> {
                                                                      var built = delegate.executorServiceFactory()
                                                                                          .createExecutorService(config,
                                                                                                                 queue);
                                                                      return new MeasuringExecutorServiceDecorator(
                                                                              BusType.COMMAND,
                                                                              built,
                                                                              cc.getComponent(ApplicationMetricRegistry.class));
                                                                  });
                                                      }))
                .registerDecorator(DecoratorDefinition.forType(DistributedQueryBusConfiguration.class)
                                                      .with((cc, name, delegate) -> {
                                                          return new DistributedQueryBusConfiguration(
                                                                  delegate.queryThreads(),
                                                                  (config, queue) -> {
                                                                      var built = delegate.queryExecutorServiceFactory()
                                                                                          .createExecutorService(config,
                                                                                                                 queue);
                                                                      return new MeasuringExecutorServiceDecorator(
                                                                              BusType.QUERY,
                                                                              built,
                                                                              cc.getComponent(ApplicationMetricRegistry.class));
                                                                  },
                                                                  delegate.queryResponseThreads(),
                                                                  delegate.queryResponseExecutorServiceFactory());
                                                      }));


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
