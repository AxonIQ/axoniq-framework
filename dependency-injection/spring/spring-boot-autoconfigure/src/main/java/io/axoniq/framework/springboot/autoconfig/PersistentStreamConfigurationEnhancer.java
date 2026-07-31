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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSource;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSourceFactory;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * A {@link ConfigurationEnhancer} that registers a {@link SubscribableEventSource} component consuming a persistent
 * stream for each entry in {@link AxonServerConfiguration#getPersistentStreams()}.
 * <p>
 * Each map key of the {@link AxonServerConfiguration.PersistentStreamSettings} becomes the component name (and thus the
 * Spring bean name after being exposed by the
 * {@link org.axonframework.extension.spring.config.SpringComponentRegistry}).
 * <p>
 * The resolved Axon Server stream name defaults to the map key but can be overridden via
 * {@link AxonServerConfiguration.PersistentStreamSettings#getName()}.
 * <p>
 * The resulting beans can be referenced by subscribing event processors through
 * {@code axon.eventhandling.processors.&lt;processor-name&gt;.source=&lt;stream-name&gt;}.
 * <p>
 * Example configuration:
 * <pre>{@code
 * axon:
 *   axonserver:
 *     persistent-streams:
 *       myStream:
 *         initial-segment-count: 4
 *         sequencing-policy: SequentialPerAggregatePolicy
 *         initial-position: TAIL
 *         batch-size: 10
 *
 * axon:
 *   eventhandling:
 *     processors:
 *       MyProjectionGroup:
 *         source: myStream
 * }</pre>
 *
 * <p>
 * Actual construction of each source is delegated to a {@link PersistentStreamEventSourceFactory} to allow
 * customization if needed. The default factory builds a {@link PersistentStreamEventSource}.
 * <p>
 * The {@link PersistentStreamConfigurationEnhancer} also implements {@link DisposableBean} and keeps a reference of every
 * {@link ScheduledExecutorService} a source asked it for, to shut them down again when the Spring application context
 * closes.
 * <p>
 * Marked {@link Internal} because instances are created by {@link PersistentStreamAutoConfiguration} and should not be
 * constructed directly in application code.
 *
 * @author Jakob Hatzl
 * @see PersistentStreamAutoConfiguration
 * @see PersistentStreamEventSourceFactory
 * @since 5.2.0
 */
@Internal
@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
public class PersistentStreamConfigurationEnhancer implements ConfigurationEnhancer, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(PersistentStreamConfigurationEnhancer.class);

    private final ObjectProvider<AxonServerConfiguration> axonServerConfigProvider;
    private final PersistentStreamScheduledExecutorBuilder schedulerBuilder;
    private final PersistentStreamEventSourceFactory factory;
    // Pools are added whenever a source is built, which is not necessarily the thread that ran the enhancement.
    private final List<ScheduledExecutorService> schedulers = new CopyOnWriteArrayList<>();

    /**
     * Instantiates a {@code PersistentStreamConfigurationEnhancer}.
     *
     * @param axonServerConfigProvider provider for the Axon Server configuration containing persistent stream settings;
     *                                 resolved lazily to avoid Spring lifecycle ordering issues with
     *                                 {@link org.axonframework.extension.spring.config.SpringComponentRegistry}
     * @param schedulerBuilder         the builder used to create a per-stream {@link ScheduledExecutorService}
     * @param eventSourceFactory       the eventSourceFactory used to construct each event source
     */
    public PersistentStreamConfigurationEnhancer(
            ObjectProvider<AxonServerConfiguration> axonServerConfigProvider,
            PersistentStreamScheduledExecutorBuilder schedulerBuilder,
            PersistentStreamEventSourceFactory eventSourceFactory
    ) {
        this.axonServerConfigProvider = Objects.requireNonNull(axonServerConfigProvider,
                                                               "axonServerConfigProvider must not be null");
        this.schedulerBuilder = Objects.requireNonNull(schedulerBuilder, "schedulerBuilder must not be null");
        this.factory = Objects.requireNonNull(eventSourceFactory, "eventSourceFactory must not be null");
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        AxonServerConfiguration axonServerConfig = axonServerConfigProvider.getObject();
        registerConfiguredEventSources(registry, axonServerConfig);
        configureAutoPersistentStreams(registry, axonServerConfig);
    }

    private void configureAutoPersistentStreams(ComponentRegistry registry, AxonServerConfiguration axonServerConfig) {
        if (axonServerConfig.isAutoPersistentStreamsEnabled()) {
            AxonServerConfiguration.PersistentStreamSettings autoPersistentStreamsSettings = axonServerConfig.getAutoPersistentStreamsSettings();
            registry.registerDecorator(
                    DecoratorDefinition
                            .forType(SubscribingEventProcessorConfiguration.class)
                            .with((axonConfig, name, delegate) -> {
                                String desiredStreamName = delegate.processorName() + "-stream";
                                if (!registry.hasComponent(SubscribableEventSource.class, desiredStreamName)) {
                                    PersistentStreamProperties properties = toPersistentStreamProperties(
                                            desiredStreamName,
                                            autoPersistentStreamsSettings);
                                    SubscribableEventSource streamSource = factory.build(
                                            desiredStreamName,
                                            properties,
                                            trackedSchedulerFactory(
                                                    autoPersistentStreamsSettings.getThreadCount()),
                                            autoPersistentStreamsSettings.getBatchSize(),
                                            axonConfig
                                    );
                                    // note that auto-persistent-streams event sources are NOT registered as
                                    // spring beans, because decoration runs after beans components are exposed to
                                    // the SpringComponentRegistry, but they don't need to be.
                                    registry.registerComponent(
                                            ComponentDefinition.ofTypeAndName(SubscribableEventSource.class,
                                                                              desiredStreamName)
                                                               .withBuilder(config -> streamSource)
                                    );
                                    delegate.eventSource(streamSource);
                                } else {
                                    logger.warn("""
                                                     Persistent stream source <{}> explicitly configured while \
                                                     auto-persistent-streams is enabled. The preconfigured stream \
                                                     source will be reused.""", desiredStreamName);
                                }
                                return delegate;
                            })
            );
        }
    }

    private void registerConfiguredEventSources(ComponentRegistry registry, AxonServerConfiguration axonServerConfig) {
        for (Map.Entry<String, AxonServerConfiguration.PersistentStreamSettings> entry
                : axonServerConfig.getPersistentStreams().entrySet()) {
            String beanName = entry.getKey();
            AxonServerConfiguration.PersistentStreamSettings settings = entry.getValue();
            String streamName = settings.getName() != null ? settings.getName() : beanName;

            PersistentStreamProperties properties = toPersistentStreamProperties(streamName, settings);

            registry.registerComponent(
                    ComponentDefinition.ofTypeAndName(SubscribableEventSource.class, beanName)
                                       .withBuilder(config -> factory.build(
                                               streamName,
                                               properties,
                                               trackedSchedulerFactory(settings.getThreadCount()),
                                               settings.getBatchSize(),
                                               config
                                       ))
            );
        }
    }

    /**
     * Returns a scheduler factory creating pools of the given {@code threadCount}, keeping a reference to every pool it
     * hands out so {@link #destroy()} can shut them all down again.
     * <p>
     * A factory rather than a ready-made pool, because a {@link PersistentStreamEventSourceFactory} building more than
     * one stream from a single declaration, as the multi-tenant one does per tenant, needs a pool per stream. Creating
     * them on request also means no pool exists for a source that was never built.
     *
     * @param threadCount the number of threads each pool this factory creates holds
     * @return a scheduler factory whose pools are shut down when the application context closes
     */
    private Function<String, ScheduledExecutorService> trackedSchedulerFactory(int threadCount) {
        return poolName -> {
            ScheduledExecutorService scheduler = schedulerBuilder.build(threadCount, poolName);
            schedulers.add(scheduler);
            return scheduler;
        };
    }

    @Override
    public void destroy() {
        schedulers.forEach(ScheduledExecutorService::shutdown);
    }

    private static PersistentStreamProperties toPersistentStreamProperties(
            String streamName,
            AxonServerConfiguration.PersistentStreamSettings settings
    ) {
        return new PersistentStreamProperties(
                streamName,
                settings.getInitialSegmentCount(),
                settings.getSequencingPolicy(),
                settings.getSequencingPolicyParameters(),
                settings.getInitialPosition(),
                settings.getFilter()
        );
    }
}
