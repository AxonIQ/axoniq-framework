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

package io.axoniq.framework.kafka.interop;

import io.axoniq.framework.kafka.codec.CloudEventKafkaEventCodec;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.publishing.KafkaEventSink;
import io.axoniq.framework.kafka.publishing.KafkaEventSinkConfiguration;
import io.axoniq.framework.kafka.publishing.TopicResolver;
import io.axoniq.framework.kafka.streaming.KafkaStreamableEventSource;
import io.axoniq.framework.kafka.streaming.KafkaStreamableEventSourceConfiguration;
import io.axoniq.framework.kafka.utils.KafkaTestBroker;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Exchanges events through Kafka with clients that only know CloudEvents.
 */
class CloudEventKafkaEventCodecIT {

    private final CloudEventKafkaEventCodec codec = new CloudEventKafkaEventCodec(
            new DelegatingEventConverter(new JacksonConverter()), URI.create("/parcel-service")
    );
    private final List<Runnable> cleanup = new ArrayList<>();
    private String topic;

    record ParcelDispatched(String parcelId) {

    }

    @BeforeEach
    void setUp() {
        topic = KafkaTestBroker.createTopic(KafkaTestBroker.uniqueTopic("cloudevents"), 1);
    }

    @AfterEach
    void tearDown() {
        cleanup.reversed().forEach(Runnable::run);
    }

    @Test
    void plainCloudEventsConsumerReadsPublishedEvents() {
        // given
        KafkaEventSink sink = new KafkaEventSink(KafkaTestBroker.clientFactory(),
                                                 KafkaEventSinkConfiguration.create(codec, TopicResolver.fixed(topic)));
        cleanup.add(() -> sink.shutdown().orTimeout(30, TimeUnit.SECONDS).join());
        EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched("p-1"));

        // when
        sink.publish(null, event).orTimeout(30, TimeUnit.SECONDS).join();

        // then
        Map<String, Object> properties = Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaTestBroker.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "cloudevents-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
        List<CloudEvent> received = new ArrayList<>();
        try (KafkaConsumer<String, CloudEvent> consumer =
                     new KafkaConsumer<>(properties, new StringDeserializer(), new CloudEventDeserializer())) {
            consumer.subscribe(List.of(topic));
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> received.add(r.value()));
                return !received.isEmpty();
            });
        }
        assertThat(received).singleElement().satisfies(cloudEvent -> {
            assertThat(cloudEvent.getId()).isEqualTo(event.identifier());
            assertThat(cloudEvent.getType()).isEqualTo(event.type().name());
        });
    }

    @Test
    void eventSourceReadsEventsFromPlainCloudEventsProducer() {
        // given
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                                                 .withId("ce-1")
                                                 .withSource(URI.create("/billing"))
                                                 .withType("com.example.InvoiceSent")
                                                 .withData("application/json", "{\"invoice\":42}".getBytes())
                                                 .build();
        try (KafkaProducer<String, CloudEvent> producer = new KafkaProducer<>(
                Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaTestBroker.bootstrapServers()),
                new StringSerializer(), new CloudEventSerializer())) {
            producer.send(new ProducerRecord<>(topic, "key", cloudEvent));
        }
        KafkaStreamableEventSource source = new KafkaStreamableEventSource(
                KafkaTestBroker.clientFactory(),
                KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic))
        );
        cleanup.add(() -> source.shutdown().orTimeout(30, TimeUnit.SECONDS).join());

        // when
        MessageStream<EventMessage> stream = source.open(StreamingCondition.startingFrom(null), null);
        cleanup.add(stream::close);

        // then
        List<EventMessage> events = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            Optional<MessageStream.Entry<EventMessage>> next;
            while ((next = stream.next()).isPresent()) {
                events.add(next.get().message());
            }
            return !events.isEmpty();
        });
        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.identifier()).isEqualTo("ce-1");
            assertThat(event.type().name()).isEqualTo("com.example.InvoiceSent");
        });
    }
}
