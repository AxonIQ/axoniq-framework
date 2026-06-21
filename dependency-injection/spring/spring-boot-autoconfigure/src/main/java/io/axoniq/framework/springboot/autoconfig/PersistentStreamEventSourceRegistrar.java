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
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.springframework.beans.factory.DisposableBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@link ConfigurationEnhancer} that registers a {@link PersistentStreamEventSource} component for each entry in
 * {@link AxonServerConfiguration#getPersistentStreams()}.
 * <p>
 * Each map key of the {@link AxonServerConfiguration.PersistentStreamSettings} becomes the component name (and thus the
 * Spring bean name after being exposed by the
 * {@link org.axonframework.extension.spring.config.SpringComponentRegistry}).
 * <p>
 * The resolved Axon Server stream name defaults to the map key but can be overridden via
 * {@link AxonServerConfiguration.PersistentStreamSettings#getName()}.
 * <p>
 * The resulting beans implement {@link org.axonframework.messaging.core.SubscribableEventSource} and can be referenced
 * by subscribing event processors through
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
 * Actual construction of each {@link PersistentStreamEventSource} is delegated to a
 * {@link PersistentStreamEventSourceFactory} to allow customization if needed.
 * <p>
 * The {@link PersistentStreamEventSourceRegistrar} also implements {@link DisposableBean} and keeps a reference of all
 * {@link ScheduledExecutorService} instances created during {@link #enhance(ComponentRegistry)} to shut them down again
 * when the Spring application context closes.
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
public class PersistentStreamEventSourceRegistrar implements ConfigurationEnhancer, DisposableBean {

    private final AxonServerConfiguration axonServerConfig;
    private final PersistentStreamScheduledExecutorBuilder schedulerBuilder;
    private final PersistentStreamEventSourceFactory factory;
    private final List<ScheduledExecutorService> schedulers = new ArrayList<>();

    /**
     * Instantiates a {@code PersistentStreamEventSourceRegistrar}.
     *
     * @param axonServerConfig the Axon Server configuration containing persistent stream settings
     * @param schedulerBuilder the builder used to create a per-stream {@link ScheduledExecutorService}
     * @param factory          the factory used to construct each {@link PersistentStreamEventSource}
     */
    public PersistentStreamEventSourceRegistrar(
            AxonServerConfiguration axonServerConfig,
            PersistentStreamScheduledExecutorBuilder schedulerBuilder,
            PersistentStreamEventSourceFactory factory
    ) {
        this.axonServerConfig = axonServerConfig;
        this.schedulerBuilder = schedulerBuilder;
        this.factory = factory;
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        for (Map.Entry<String, AxonServerConfiguration.PersistentStreamSettings> entry
                : axonServerConfig.getPersistentStreams().entrySet()) {
            String beanName = entry.getKey();
            AxonServerConfiguration.PersistentStreamSettings settings = entry.getValue();
            String streamName = settings.getName() != null ? settings.getName() : beanName;

            PersistentStreamProperties properties = toPersistentStreamProperties(streamName, settings);
            ScheduledExecutorService scheduler = schedulerBuilder.build(settings.getThreadCount(), streamName);
            schedulers.add(scheduler);

            registry.registerComponent(
                    ComponentDefinition.ofTypeAndName(PersistentStreamEventSource.class, beanName)
                                       .withBuilder(config -> factory.build(
                                               streamName,
                                               properties,
                                               scheduler,
                                               settings.getBatchSize(),
                                               config
                                       ))
            );
        }
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
