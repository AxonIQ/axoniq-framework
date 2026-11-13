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

package io.axoniq.framework.kafka.common;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

class KafkaClientFactoryTest {

    // Nothing listens here; creating a client does not connect.
    private final KafkaClientFactory testSubject = KafkaClientFactory.create(Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
            CommonClientConfigs.CLIENT_ID_CONFIG, "shared"
    ));

    private static Set<String> clientIds(Map<MetricName, ? extends Metric> metrics) {
        return metrics.keySet().stream()
                      .map(name -> name.tags().get("client-id"))
                      .filter(Objects::nonNull)
                      .collect(Collectors.toSet());
    }

    @Test
    void rejectsASharedTransactionalIdentifier() {
        // when / then - producers that share a transactional identifier fence each other off
        assertThatThrownBy(() -> KafkaClientFactory.create(Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "shared"
        ))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("transactional.id");
    }

    @Test
    void rejectsASharedGroupInstanceIdentifier() {
        // when / then - consumers that share a group instance identifier fence each other off
        assertThatThrownBy(() -> KafkaClientFactory.create(Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "shared"
        ))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("group.instance.id");
    }

    @Test
    void producerUsesSharedConfiguration() {
        // when
        Producer<String, String> producer =
                testSubject.createProducer(Map.of(), new StringSerializer(), new StringSerializer());

        // then
        try {
            assertThat(clientIds(producer.metrics())).containsExactly("shared");
        } finally {
            // closed without waiting: nothing listens, so there is nothing to wait for
            producer.close(Duration.ZERO);
        }
    }

    @Test
    void componentPropertiesTakePrecedenceOverSharedConfiguration() {
        // when
        try (Consumer<String, String> consumer = testSubject.createConsumer(
                Map.of(CommonClientConfigs.CLIENT_ID_CONFIG, "specific"),
                new StringDeserializer(), new StringDeserializer())) {
            // then
            assertThat(clientIds(consumer.metrics())).containsExactly("specific");
        }
    }

    @Nested
    class ComponentClients {

        @Test
        void everyClientOfAComponentGetsAClientIdentifierOfItsOwn() {
            // given
            KafkaClientFactory component = testSubject.forComponent("outbound");

            // when
            Producer<String, String> first =
                    component.createProducer(Map.of(), new StringSerializer(), new StringSerializer());
            Producer<String, String> second =
                    component.createProducer(Map.of(), new StringSerializer(), new StringSerializer());

            // then
            try {
                assertThat(clientIds(first.metrics())).containsExactly("shared-outbound-0");
                assertThat(clientIds(second.metrics())).containsExactly("shared-outbound-1");
            } finally {
                first.close(Duration.ZERO);
                second.close(Duration.ZERO);
            }
        }

        @Test
        void aClientIdentifierOfTheComponentIsSuffixedAsWell() {
            // when
            try (Consumer<String, String> consumer = testSubject.forComponent("parcel-group").createConsumer(
                    Map.of(CommonClientConfigs.CLIENT_ID_CONFIG, "billing"),
                    new StringDeserializer(), new StringDeserializer())) {
                // then
                assertThat(clientIds(consumer.metrics())).containsExactly("billing-parcel-group-0");
            }
        }

        @Test
        void aClientOfAComponentKeepsItsClientIdentifierWhenCreatedAnew() {
            // given
            KafkaClientFactory member = testSubject.forComponent("parcel-group").forClient("0");
            Consumer<String, String> first =
                    member.createConsumer(Map.of(), new StringDeserializer(), new StringDeserializer());
            String firstClientId;
            try (first) {
                firstClientId = String.join(",", clientIds(first.metrics()));
            }

            // when - the client is replaced, as after a failure
            try (Consumer<String, String> replacement =
                         member.createConsumer(Map.of(), new StringDeserializer(), new StringDeserializer())) {
                // then
                assertThat(firstClientId).isEqualTo("shared-parcel-group-0");
                assertThat(clientIds(replacement.metrics())).containsExactly("shared-parcel-group-0");
            }
        }

        @Test
        void aClientNameReplacesTheNumberingOfTheComponentsClients() {
            // given
            KafkaClientFactory component = testSubject.forComponent("parcel-group");

            // when
            Producer<String, String> deadLetters = component.forClient("dlt")
                                                            .createProducer(Map.of(),
                                                                            new StringSerializer(),
                                                                            new StringSerializer());
            Producer<String, String> numbered =
                    component.createProducer(Map.of(), new StringSerializer(), new StringSerializer());

            // then
            try {
                assertThat(clientIds(deadLetters.metrics())).containsExactly("shared-parcel-group-dlt");
                assertThat(clientIds(numbered.metrics())).containsExactly("shared-parcel-group-0");
            } finally {
                deadLetters.close(Duration.ZERO);
                numbered.close(Duration.ZERO);
            }
        }

        @Test
        void aClientNameWithoutComponentSuffixesTheSharedClientIdentifier() {
            // when
            Producer<String, String> producer = testSubject.forClient("audit")
                                                           .createProducer(Map.of(),
                                                                           new StringSerializer(),
                                                                           new StringSerializer());

            // then
            try {
                assertThat(clientIds(producer.metrics())).containsExactly("shared-audit");
            } finally {
                producer.close(Duration.ZERO);
            }
        }

        @Test
        void clientsWithoutClientIdentifierGetNone() {
            // given
            KafkaClientFactory withoutClientId =
                    KafkaClientFactory.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1"));

            // when
            Admin admin = withoutClientId.forComponent("parcel-stream").createAdmin(Map.of());

            // then - Kafka generates one, rather than the component suffix standing alone
            try {
                assertThat(clientIds(admin.metrics())).noneMatch(id -> id.startsWith("-parcel-stream"));
            } finally {
                admin.close(Duration.ZERO);
            }
        }
    }

    @Test
    void adminUsesSharedConfiguration() {
        // when
        Admin admin = testSubject.createAdmin(Map.of());

        // then
        try {
            assertThat(clientIds(admin.metrics())).containsExactly("shared");
        } finally {
            admin.close(Duration.ZERO);
        }
    }
}
