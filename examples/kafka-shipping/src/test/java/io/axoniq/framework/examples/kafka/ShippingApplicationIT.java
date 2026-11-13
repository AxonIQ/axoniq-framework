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

package io.axoniq.framework.examples.kafka;

import io.axoniq.framework.examples.kafka.shipping.OrderShipped;
import io.axoniq.framework.examples.kafka.tracking.ParcelTracking;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Runs the shipping service against a Kafka broker in a container.
 */
@SpringBootTest
class ShippingApplicationIT {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.3.1");

    @Autowired
    private EventGateway eventGateway;
    @Autowired
    private ParcelTracking tracking;

    @DynamicPropertySource
    static void kafka(DynamicPropertyRegistry registry) throws Exception {
        KAFKA.start();
        try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                                               KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("parcels", 4, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        registry.add("axon.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @AfterAll
    static void stopKafka() {
        KAFKA.stop();
    }

    @Test
    void internationalShipmentsReachTheTrackingProjectionThroughKafka() {
        // given
        // the Kafka container and the parcels topic are set up in the @DynamicPropertySource method

        // when
        eventGateway.publish(List.of(new OrderShipped("order-1", "parcel-1", "NL", true),
                                     new OrderShipped("order-2", "parcel-2", "BE", false)))
                    .orTimeout(30, TimeUnit.SECONDS).join();

        // then
        await().atMost(Duration.ofSeconds(60)).until(() -> tracking.destinationOf("parcel-1").isPresent());
        assertThat(tracking.destinationOf("parcel-1")).contains("NL");
        // the domestic shipment was never published
        await().during(Duration.ofSeconds(2)).until(() -> tracking.destinationOf("parcel-2").isEmpty());
    }
}
