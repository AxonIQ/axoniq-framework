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
package io.axoniq.framework.springboot.kafka;

import io.axoniq.framework.kafka.configuration.KafkaConnectorStatus;
import io.axoniq.framework.kafka.subscribing.KafkaSourceStatus;
import io.axoniq.framework.springboot.actuator.kafka.KafkaConnectorHealthIndicator;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs a Spring Boot application whose subscribing Kafka source cannot create its consumers, because one of its
 * consumer settings is invalid, and verifies that the Kafka connector's health indicator reports it. Creating a
 * consumer validates its settings before it connects, so no broker is needed.
 *
 * @author Allard Buijze
 */
@SpringBootTest(classes = KafkaConnectorHealthIndicatorTest.TestApplication.class, properties = {
        "axon.axonserver.enabled=false",
        "axon.postgresql.enabled=false",
        "spring.main.banner-mode=off",
        "axon.kafka.subscribing-sources.parcels.topics=parcels",
        "axon.kafka.subscribing-sources.parcels.group-id=billing",
        "axon.kafka.subscribing-sources.parcels.properties.session.timeout.ms=not-a-number",
        "axon.kafka.subscribing-sources.unused.topics=parcels",
        "axon.kafka.subscribing-sources.unused.group-id=auditing",
        "axon.eventhandling.processors[io.axoniq.framework.springboot.kafka].mode=subscribing",
        "axon.eventhandling.processors[io.axoniq.framework.springboot.kafka].source=parcels"
})
class KafkaConnectorHealthIndicatorTest {

    @Autowired
    private KafkaConnectorHealthIndicator healthIndicator;

    @Autowired
    private KafkaConnectorStatus status;

    @Event(namespace = "parcels", name = "ParcelDispatched", version = "1.0")
    public record ParcelDispatched(String parcelId) {

    }

    /**
     * Handles nothing; it only gives the subscribing processor a handler, so that it subscribes to its source.
     */
    public static class ParcelListener {

        @EventHandler
        public void on(ParcelDispatched event) {
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {JmxAutoConfiguration.class, WebClientAutoConfiguration.class})
    static class TestApplication {

        @Bean
        ParcelListener parcelListener() {
            return new ParcelListener();
        }
    }

    @Test
    void reportsDownWithTheFailedSourceAndItsCause() {
        // when
        await().atMost(Duration.ofSeconds(10)).until(() -> !status.healthy());
        Health health = healthIndicator.health();

        // then
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsKey("sources");
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> sources = (Map<String, Map<String, Object>>) health.getDetails()
                                                                                           .get("sources");
        assertThat(sources.get("parcels")).containsEntry("status", "FAILED");
        assertThat((String) sources.get("parcels").get("error")).contains("ConfigException")
                                                                .contains("session.timeout.ms");
        assertThat(sources.get("unused")).containsEntry("status", "STOPPED");
        assertThat(status.source("parcels")).hasValueSatisfying(
                source -> assertThat(source).isInstanceOf(KafkaSourceStatus.Failed.class)
        );
    }
}
