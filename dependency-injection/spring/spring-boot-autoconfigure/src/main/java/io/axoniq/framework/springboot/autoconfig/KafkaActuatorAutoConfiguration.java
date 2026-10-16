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

import io.axoniq.framework.kafka.configuration.KafkaConnector;
import io.axoniq.framework.kafka.configuration.KafkaConnectorStatus;
import io.axoniq.framework.springboot.actuator.kafka.KafkaConnectorHealthIndicator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration of the Spring Boot Actuator health indicator of the Kafka connector, reporting a subscribable
 * Kafka source that stopped because of a failure that retrying cannot resolve.
 * <p>
 * Active when Spring Boot Actuator and the {@code axoniq-kafka} module are on the classpath, the connector is enabled,
 * and {@code management.health.kafka-connector.enabled} is not {@code false}. An application bean of type
 * {@link KafkaConnectorHealthIndicator} replaces the autoconfigured one.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@AutoConfiguration(after = KafkaAutoConfiguration.class)
@ConditionalOnClass(name = {
        "org.springframework.boot.actuate.health.AbstractHealthIndicator",
        "io.axoniq.framework.kafka.configuration.KafkaConnector"
})
@ConditionalOnProperty(name = "axon.kafka.enabled", matchIfMissing = true)
public class KafkaActuatorAutoConfiguration {

    /**
     * Creates the health indicator reporting the status of the subscribable Kafka sources, as the
     * {@link KafkaConnectorStatus} component that the {@link KafkaConnector} registers provides it.
     *
     * @param status the provider of the Kafka connector's status
     * @return the health indicator of the Kafka connector
     */
    @Bean
    @ConditionalOnProperty(name = "management.health.kafka-connector.enabled", matchIfMissing = true)
    @ConditionalOnMissingBean(KafkaConnectorHealthIndicator.class)
    public KafkaConnectorHealthIndicator kafkaConnectorHealthIndicator(ObjectProvider<KafkaConnectorStatus> status) {
        return new KafkaConnectorHealthIndicator(status);
    }
}
