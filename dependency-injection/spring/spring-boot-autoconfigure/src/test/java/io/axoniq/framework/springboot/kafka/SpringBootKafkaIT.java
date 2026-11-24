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

import io.axoniq.framework.kafka.codec.DefaultKafkaEventCodec;
import io.axoniq.framework.kafka.common.KafkaResources;
import io.axoniq.framework.kafka.publishing.EventSinks;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.annotation.AnnotationMessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventBus;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.tracing.TracingEventSink;
import org.axonframework.messaging.tracing.LoggingSpanFactory;
import org.axonframework.messaging.tracing.SpanFactory;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs a Spring Boot application with Kafka sinks and sources configured through properties.
 * <p>
 * The application context is closed after this class, while the broker still runs, so that its consumers and
 * producers shut down cleanly; Testcontainers removes the broker when the test run ends.
 *
 * @author Allard Buijze
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(classes = SpringBootKafkaIT.TestApplication.class, properties = {
        "axon.axonserver.enabled=false",
        "axon.postgresql.enabled=false",
        "spring.main.banner-mode=off",
        "axon.kafka.sinks.outbound.topic=" + SpringBootKafkaIT.OUTBOUND,
        "axon.kafka.streaming-sources.parcels.topics=" + SpringBootKafkaIT.INBOUND,
        "axon.eventhandling.processors[io.axoniq.framework.springboot.kafka].source=parcels"
})
class SpringBootKafkaIT {

    static final String INBOUND = "spring-inbound";
    static final String OUTBOUND = "spring-outbound";

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.3.1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");

    private static final DefaultKafkaEventCodec CODEC =
            new DefaultKafkaEventCodec(new DelegatingEventConverter(new JacksonConverter()));

    @Autowired
    private ParcelForwarder forwarder;

    @Autowired
    private EventSink applicationSink;

    @Autowired
    @Qualifier("outbound")
    private EventSink kafkaSink;

    private static final MessageTypeResolver TYPES = new AnnotationMessageTypeResolver();
    private static final List<String> FORWARDED = new CopyOnWriteArrayList<>();
    private static @Nullable KafkaConsumer<String, byte[]> verifier;

    @Event(namespace = "parcels", name = "ParcelDispatched", version = "1.0")
    public record ParcelDispatched(String parcelId) {

    }

    @Event(namespace = "parcels", name = "ParcelForwarded", version = "1.0")
    public record ParcelForwarded(String parcelId) {

    }

    /**
     * Receives parcels from the inbound topic, and forwards them through the configured sink, which Spring injects by
     * name. It also records the unqualified event sink its handler receives, which must be the application's own.
     */
    public static class ParcelForwarder {

        final List<String> received = new CopyOnWriteArrayList<>();
        final List<EventSink> unqualifiedSinks = new CopyOnWriteArrayList<>();
        private final EventSink kafka;

        ParcelForwarder(EventSink kafka) {
            this.kafka = kafka;
        }

        @EventHandler
        public CompletableFuture<Void> on(ParcelDispatched event,
                                          EventSink applicationSink,
                                          MessageTypeResolver typeResolver,
                                          ProcessingContext context) {
            received.add(event.parcelId() + "@" + context.getResource(KafkaResources.TOPIC_PARTITION).topic());
            unqualifiedSinks.add(applicationSink);
            return EventSinks.publishPayloads(kafka, context, typeResolver, new ParcelForwarded(event.parcelId()));
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {JmxAutoConfiguration.class, WebClientAutoConfiguration.class})
    static class TestApplication {

        @Bean
        ParcelForwarder parcelForwarder(@Qualifier("outbound") EventSink kafka) {
            return new ParcelForwarder(kafka);
        }

        // tracing decorates the application's event sinks; the Kafka sink must keep working alongside it
        @Bean
        SpanFactory spanFactory() {
            return LoggingSpanFactory.INSTANCE;
        }
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        KAFKA.start();
        try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                                               KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(INBOUND, 2, (short) 1), new NewTopic(OUTBOUND, 1, (short) 1)))
                 .all().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating the topics", e);
        } catch (Exception e) {
            throw new IllegalStateException("Creating the topics failed", e);
        }
        registry.add("axon.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @AfterAll
    static void closeVerifier() {
        KafkaConsumer<String, byte[]> current = verifier;
        if (current != null) {
            current.close(Duration.ofSeconds(5));
        }
    }

    @Test
    void processorReadsConfiguredSourceAndPublishesThroughConfiguredSink() {
        // given
        String parcelId = UUID.randomUUID().toString();
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
                Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
                new StringSerializer(), new ByteArraySerializer())) {
            // when
            ParcelDispatched payload = new ParcelDispatched(parcelId);
            producer.send(CODEC.encode(new GenericEventMessage(TYPES.resolveOrThrow(payload), payload),
                                       INBOUND, parcelId, null));
        }

        // then
        await().atMost(Duration.ofSeconds(60)).until(() -> forwarder.received.contains(parcelId + "@" + INBOUND));
        await().atMost(Duration.ofSeconds(30)).pollInSameThread().until(() -> forwardedParcels().contains(parcelId));
        assertThat(forwarder.unqualifiedSinks).isNotEmpty()
                                              .allSatisfy(sink -> assertThat(sink).isSameAs(applicationSink));
    }

    @Test
    void unqualifiedEventSinkIsTheApplicationsOwnAndTheNamedSinkIsDecorated() {
        // then
        assertThat(applicationSink).isInstanceOf(EventBus.class);
        assertThat(kafkaSink).isNotSameAs(applicationSink)
                             .isInstanceOf(TracingEventSink.class);
    }

    /**
     * Returns the parcels forwarded to the outbound topic so far, read by one consumer that keeps its position
     * between calls.
     */
    private static List<String> forwardedParcels() {
        KafkaConsumer<String, byte[]> consumer = verifier;
        if (consumer == null) {
            Map<String, Object> properties = Map.of(
                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
            );
            consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
            consumer.subscribe(List.of(OUTBOUND));
            verifier = consumer;
        }
        consumer.poll(Duration.ofMillis(200)).forEach(record -> CODEC.decode(record).ifPresent(
                decoded -> FORWARDED.add(decoded.event().payloadAs(ParcelForwarded.class).parcelId())
        ));
        return FORWARDED;
    }
}
