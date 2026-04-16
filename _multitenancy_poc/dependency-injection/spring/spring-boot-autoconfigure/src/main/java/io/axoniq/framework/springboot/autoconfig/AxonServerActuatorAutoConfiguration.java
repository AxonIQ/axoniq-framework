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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.springboot.actuator.axonserver.AxonServerHealthIndicator;
import io.axoniq.framework.springboot.actuator.axonserver.AxonServerStatusAggregator;
import org.springframework.boot.actuate.health.SimpleStatusAggregator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration class for Spring Boot Actuator monitoring tools around Axon Server.
 *
 * @author Steven van Beelen
 * @since 4.6.0
 */
@AutoConfiguration(after = AxonServerAutoConfiguration.class)
@ConditionalOnClass(name = {
        "org.springframework.boot.actuate.health.AbstractHealthIndicator",
        "org.axonframework.axonserver.connector.AxonServerConnectionManager"
})
@ConditionalOnProperty(name = "axon.axonserver.enabled", matchIfMissing = true)
public class AxonServerActuatorAutoConfiguration {

    @ConditionalOnMissingBean(AxonServerHealthIndicator.class)
    @Bean
    public AxonServerHealthIndicator axonServerHealthIndicator(AxonServerConnectionManager connectionManager) {
        return new AxonServerHealthIndicator(connectionManager);
    }
    @ConditionalOnMissingBean(SimpleStatusAggregator.class)
    @Bean
    public AxonServerStatusAggregator axonServerStatusAggregator() {
        return new AxonServerStatusAggregator();
    }
}
