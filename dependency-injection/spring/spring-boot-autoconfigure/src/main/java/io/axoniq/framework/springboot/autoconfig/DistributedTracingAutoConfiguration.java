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
 * The generic tracing properties (master switch, per-component toggles, attribute providers) are handled by the
 * open-source {@code TracingAutoConfiguration} from the Axon Framework Spring Boot autoconfigure module; this
 * configuration adds only the {@link DistributedTracingSettings} translation and activates when the
 * {@code axoniq-distributed-messaging} module is on the classpath. Registration uses {@code registerIfNotPresent},
 * so a user-defined settings bean or component always wins. The configuration backs off when
 * {@code axon.tracing.enabled} is set to {@code false}.
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
                c -> new DistributedTracingSettings(properties.getCommandBusConnector().isEnabled(),
                                                    properties.getQueryBusConnector().isEnabled())
        );
    }
}
