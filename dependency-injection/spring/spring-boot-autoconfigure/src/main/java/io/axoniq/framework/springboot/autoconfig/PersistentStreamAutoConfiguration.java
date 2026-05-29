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

import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Spring Boot autoconfiguration for Persistent Streams backed by Axon Server.
 * <p>
 * Registers a {@link PersistentStreamScheduledExecutorBuilder} and a
 * {@link PersistentStreamMessageSourceRegistrar} that reads
 * {@code axon.axonserver.persistent-streams.*} properties and creates a
 * {@link io.axoniq.framework.axonserver.connector.event.PersistentStreamMessageSource} Spring bean for each
 * configured stream. Those beans are resolved by name in the event-processing configuration through
 * {@code axon.eventhandling.processors.<name>.source=<stream-key>}.
 * <p>
 * This configuration is only active when {@code axon.axonserver.enabled} is {@code true} (the default).
 *
 * @author Marc Gathier
 * @author Steven van Beelen
 * @since 5.2.0
 */
@AutoConfiguration(after = AxonServerAutoConfiguration.class)
public class PersistentStreamAutoConfiguration {

    /**
     * Creates a {@link PersistentStreamScheduledExecutorBuilder} that constructs a new
     * {@link ScheduledExecutorService} for each persistent stream.
     * <p>
     * This bean is only created when no {@link PersistentStreamScheduledExecutorBuilder} bean exists and no bean
     * named {@code "persistentStreamScheduler"} is present. When a {@code "persistentStreamScheduler"} bean
     * exists, {@link #backwardsCompatiblePersistentStreamScheduledExecutorBuilder(ScheduledExecutorService)} is used
     * instead.
     *
     * @return the default {@link PersistentStreamScheduledExecutorBuilder}
     */
    @Bean
    @ConditionalOnMissingBean(value = PersistentStreamScheduledExecutorBuilder.class,
            name = "persistentStreamScheduler")
    @ConditionalOnProperty(name = "axon.axonserver.enabled", matchIfMissing = true)
    public PersistentStreamScheduledExecutorBuilder persistentStreamScheduledExecutorBuilder() {
        return PersistentStreamScheduledExecutorBuilder.defaultFactory();
    }

    /**
     * Creates a {@link PersistentStreamScheduledExecutorBuilder} that always returns the same
     * {@link ScheduledExecutorService}, for backwards compatibility with Axon Framework 4.10.0.
     * <p>
     * This bean is only created when a bean named {@code "persistentStreamScheduler"} of type
     * {@link ScheduledExecutorService} is present.
     *
     * @param persistentStreamScheduler the shared {@link ScheduledExecutorService} to use for all streams
     * @return a {@link PersistentStreamScheduledExecutorBuilder} that always returns the given executor
     */
    @Bean
    @ConditionalOnMissingBean(PersistentStreamScheduledExecutorBuilder.class)
    @ConditionalOnBean(name = "persistentStreamScheduler")
    @ConditionalOnProperty(name = "axon.axonserver.enabled", matchIfMissing = true)
    public PersistentStreamScheduledExecutorBuilder backwardsCompatiblePersistentStreamScheduledExecutorBuilder(
            @Qualifier("persistentStreamScheduler") ScheduledExecutorService persistentStreamScheduler
    ) {
        return (threadCount, streamName) -> persistentStreamScheduler;
    }

    /**
     * Constructs a {@link PersistentStreamMessageSourceRegistrar} that reads
     * {@code axon.axonserver.persistent-streams.*} from the application environment and registers a
     * {@link io.axoniq.framework.axonserver.connector.event.PersistentStreamMessageSource} Spring bean for each
     * configured stream.
     *
     * @param environment     the Spring {@link Environment}
     * @param executorBuilder the {@link PersistentStreamScheduledExecutorBuilder} used to construct a
     *                        {@link ScheduledExecutorService} per stream
     * @return the {@link PersistentStreamMessageSourceRegistrar}
     */
    @Bean
    @ConditionalOnProperty(name = "axon.axonserver.enabled", matchIfMissing = true)
    public PersistentStreamMessageSourceRegistrar persistentStreamRegistrar(
            Environment environment,
            PersistentStreamScheduledExecutorBuilder executorBuilder
    ) {
        return new PersistentStreamMessageSourceRegistrar(environment, executorBuilder);
    }

}
