/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.messaging.distributed.tracing.DistributedTracingSettings;
import io.axoniq.framework.springboot.DistributedTracingProperties;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration translating the connector-specific {@code axon.tracing.*} properties into the
 * {@link DistributedTracingSettings} component read by the distributed-connector tracing enhancer of
 * {@code axoniq-distributed-messaging}.
 * <p>
 * The shared tracing properties (master switch, per-component toggles, and attribute providers) use the same
 * namespace. This configuration adds the {@link DistributedTracingSettings} translation and activates when the
 * {@code axoniq-distributed-messaging} module is on the classpath. Registration uses
 * {@code registerIfNotPresent}, so a user-defined settings bean or component always wins. The configuration backs off
 * when {@code axon.tracing.enabled} is set to {@code false}.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@AutoConfiguration
@ConditionalOnClass(DistributedTracingSettings.class)
@ConditionalOnProperty(prefix = "axon.tracing", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(DistributedTracingProperties.class)
public class DistributedTracingAutoConfiguration {

    /**
     * Constructs a {@link ConfigurationEnhancer} translating the connector toggles of the {@code axon.tracing.*}
     * properties into the {@link DistributedTracingSettings} component.
     *
     * @param properties the bound {@link DistributedTracingProperties}
     * @return a {@link ConfigurationEnhancer} registering the distributed tracing settings with the framework
     */
    @Bean
    public ConfigurationEnhancer distributedTracingConfigurationEnhancer(DistributedTracingProperties properties) {
        return registry -> registry.registerIfNotPresent(
                DistributedTracingSettings.class,
                c -> DistributedTracingSettings.enabledByDefault()
                                               .withCommandBusConnectorEnabled(
                                                       properties.getCommandBusConnector().isEnabled()
                                               )
                                               .withQueryBusConnectorEnabled(
                                                       properties.getQueryBusConnector().isEnabled()
                                               )
        );
    }
}
