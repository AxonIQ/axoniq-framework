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

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.event.DefaultPersistentStreamEventSourceFactory;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSource;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSourceFactory;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import org.axonframework.extension.springboot.autoconfig.EventProcessingAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot autoconfiguration that defines the required infrastructure for persistent streams by creating
 * a {@link PersistentStreamScheduledExecutorBuilder}, a {@link PersistentStreamEventSourceFactory} and a named
 * {@link PersistentStreamEventSource} bean for each entry under {@code axon.axonserver.persistent-streams} using
 * the {@link PersistentStreamEventSourceRegistrar} configuration enhancer.
 *
 * @author Jakob Hatzl
 * @since 5.2.0
 * @see PersistentStreamEventSource
 * @see AxonServerConfiguration.PersistentStreamSettings
 * @see PersistentStreamEventSourceFactory
 */
@AutoConfiguration(
        after = {AxonServerAutoConfiguration.class, DeadLetterQueueConfiguration.class},
        before = EventProcessingAutoConfiguration.class
)
@EnableConfigurationProperties(AxonServerConfiguration.class)
public class PersistentStreamAutoConfiguration {

    /**
     * Creates the default {@link PersistentStreamScheduledExecutorBuilder} if no custom one is present.
     *
     * @return the default {@link PersistentStreamScheduledExecutorBuilder}
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.axonserver.event-store.enabled", matchIfMissing = true)
    public PersistentStreamScheduledExecutorBuilder persistentStreamScheduledExecutorBuilder() {
        return PersistentStreamScheduledExecutorBuilder.defaultFactory();
    }

    /**
     * Creates the default {@link PersistentStreamEventSourceFactory} if no custom one is present.
     * <p>
     * The default implementation is {@link DefaultPersistentStreamEventSourceFactory}. Declare a bean of type
     * {@link PersistentStreamEventSourceFactory} to replace this with custom construction logic.
     * @return the default {@link PersistentStreamEventSourceFactory}
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.axonserver.event-store.enabled", matchIfMissing = true)
    public PersistentStreamEventSourceFactory persistentStreamEventSourceFactory() {
        return PersistentStreamEventSourceFactory.defaultFactory();
    }

    /**
     * Creates a {@link PersistentStreamEventSourceRegistrar} that registers a {@link PersistentStreamEventSource}
     * component for each entry under {@code axon.axonserver.persistent-streams}.
     * <p>
     * By being a {@link org.axonframework.common.configuration.ConfigurationEnhancer} each component is registered
     * under the map key as its name, making it retrievable via
     * {@code configuration.getOptionalComponent(SubscribableEventSource.class, "&lt;stream-name&gt;")}. The
     * {@link org.axonframework.extension.spring.config.SpringComponentRegistry} promotes these components to named
     * Spring beans, which enables wiring through
     * {@code axon.eventhandling.processors.&lt;name&gt;.source=&lt;stream-name&gt;}.
     * <p>
     * The returned bean also implements {@link org.springframework.beans.factory.DisposableBean} to shut down all
     * created {@link java.util.concurrent.ScheduledExecutorService} instances when the Spring application context
     * closes.
     * <p>
     *
     *
     * @param axonServerConfig the Axon Server configuration containing persistent stream settings
     * @param schedulerBuilder the builder used to create a per-stream {@link java.util.concurrent.ScheduledExecutorService}
     * @param factory          the factory used to construct each {@link PersistentStreamEventSource}
     * @return a {@link PersistentStreamEventSourceRegistrar} that registers {@link PersistentStreamEventSource}
     *         components
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.axonserver.event-store.enabled", matchIfMissing = true)
    public PersistentStreamEventSourceRegistrar persistentStreamEventSourceEnhancer(
            AxonServerConfiguration axonServerConfig,
            PersistentStreamScheduledExecutorBuilder schedulerBuilder,
            PersistentStreamEventSourceFactory factory
    ) {
        return new PersistentStreamEventSourceRegistrar(axonServerConfig, schedulerBuilder, factory);
    }
}
