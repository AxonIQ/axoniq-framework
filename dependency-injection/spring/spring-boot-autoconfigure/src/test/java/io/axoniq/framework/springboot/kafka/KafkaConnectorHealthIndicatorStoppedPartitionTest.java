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

import io.axoniq.framework.kafka.common.KafkaClientFactory;
import io.axoniq.framework.springboot.actuator.kafka.KafkaConnectorHealthIndicator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs a Spring Boot application whose subscribing Kafka source stops consuming a partition, because its committed
 * offset was lost to retention, and verifies that the Kafka connector's health indicator lists the partition while
 * reporting {@link Status#UP}. A {@link KafkaClientFactory} bean hands out Kafka's mock clients, so no broker is
 * needed.
 *
 * @author Allard Buijze
 */
@SpringBootTest(classes = KafkaConnectorHealthIndicatorStoppedPartitionTest.TestApplication.class, properties = {
        "axon.axonserver.enabled=false",
        "axon.postgresql.enabled=false",
        "spring.main.banner-mode=off",
        "axon.kafka.subscribing-sources.parcels.topics=parcels",
        "axon.kafka.subscribing-sources.parcels.group-id=billing",
        "axon.eventhandling.processors[io.axoniq.framework.springboot.kafka].mode=subscribing",
        "axon.eventhandling.processors[io.axoniq.framework.springboot.kafka].source=parcels"
})
class KafkaConnectorHealthIndicatorStoppedPartitionTest {

    private static final TopicPartition PARTITION = new TopicPartition("parcels", 0);

    @Autowired
    private KafkaConnectorHealthIndicator healthIndicator;

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

    /**
     * Hands out mock consumers whose partition hits a retention gap on the first poll.
     */
    static class RetentionGapClientFactory implements KafkaClientFactory {

        @Override
        public <K, V> Producer<K, V> createProducer(Map<String, Object> properties,
                                                    Serializer<K> keySerializer,
                                                    Serializer<V> valueSerializer) {
            return new MockProducer<>(true, null, keySerializer, valueSerializer);
        }

        @Override
        public <K, V> Consumer<K, V> createConsumer(Map<String, Object> properties,
                                                    Deserializer<K> keyDeserializer,
                                                    Deserializer<V> valueDeserializer) {
            MockConsumer<K, V> consumer = mockConsumer();
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(PARTITION));
                consumer.updateBeginningOffsets(Map.of(PARTITION, 5L));
                consumer.setPollException(new OffsetOutOfRangeException("retention gap", Map.of(PARTITION, 2L)));
            });
            return consumer;
        }

        // The constructor taking a String, which kafka-clients 4 prefers, does not exist in the 3.x line.
        @SuppressWarnings("deprecation")
        private static <K, V> MockConsumer<K, V> mockConsumer() {
            return new MockConsumer<>(OffsetResetStrategy.NONE);
        }

        @Override
        public Admin createAdmin(Map<String, Object> properties) {
            throw new UnsupportedOperationException("Not used by subscribing sources");
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {JmxAutoConfiguration.class, WebClientAutoConfiguration.class})
    static class TestApplication {

        @Bean
        ParcelListener parcelListener() {
            return new ParcelListener();
        }

        @Bean
        KafkaClientFactory kafkaClientFactory() {
            return new RetentionGapClientFactory();
        }
    }

    @Test
    void reportsUpWithThePartitionTheRunningSourceStoppedConsuming() {
        // when
        await().atMost(Duration.ofSeconds(10)).until(() -> stoppedPartitions(healthIndicator.health()) != null);
        Health health = healthIndicator.health();

        // then
        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(sources(health).get("parcels")).containsEntry("status", "RUNNING");
        assertThat(stoppedPartitions(health)).singleElement().satisfies(stopped -> {
            assertThat(stopped).containsEntry("partition", "parcels-0")
                               .containsEntry("offset", 2L)
                               .containsEntry("reason", "RETENTION_GAP");
            assertThat((String) stopped.get("error")).contains("OffsetOutOfRangeException");
        });
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Object>> sources(Health health) {
        return (Map<String, Map<String, Object>>) health.getDetails().get("sources");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> stoppedPartitions(Health health) {
        return (List<Map<String, Object>>) sources(health).get("parcels").get("stoppedPartitions");
    }
}
