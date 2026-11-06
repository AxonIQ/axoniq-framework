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

package io.axoniq.framework.kafka.configuration;

import io.axoniq.framework.kafka.codec.CodecTestFixtures;
import io.axoniq.framework.kafka.codec.DefaultKafkaEventCodec;
import io.axoniq.framework.kafka.common.KafkaResources;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.publishing.EventSinks;
import io.axoniq.framework.kafka.publishing.TopicResolver;
import io.axoniq.framework.kafka.utils.KafkaTestBroker;
import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import org.apache.kafka.clients.CommonClientConfigs;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.annotation.AnnotationMessageTypeResolver;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class KafkaConnectorIT {

    private final DefaultKafkaEventCodec codec =
            CodecTestFixtures.defaultCodec();
    private @Nullable AxonConfiguration configuration;
    private String inbound;
    private String outbound;

    private static final MessageTypeResolver TYPES = new AnnotationMessageTypeResolver();

    @Event(namespace = "parcels", name = "ParcelDispatched", version = "1.0")
    public record ParcelDispatched(String parcelId) {

    }

    @Event(namespace = "parcels", name = "ParcelForwarded", version = "1.0")
    public record ParcelForwarded(String parcelId) {

    }

    @BeforeEach
    void setUp() {
        inbound = KafkaTestBroker.createTopic(KafkaTestBroker.uniqueTopic("inbound"), 2);
        outbound = KafkaTestBroker.createTopic(KafkaTestBroker.uniqueTopic("outbound"), 1);
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    private KafkaConnector connector() {
        return KafkaConnector.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                                            KafkaTestBroker.bootstrapServers()));
    }

    private static StreamableEventSource parcels(Configuration config) {
        return config.getComponent(StreamableEventSource.class, "parcels");
    }

    private static EventMessage event(Object payload) {
        return new GenericEventMessage(TYPES.resolveOrThrow(payload), payload);
    }

    private void publishInbound(EventMessage event) {
        KafkaTestBroker.produce(List.of(codec.encode(event, inbound, null, null)));
    }

    /**
     * Forwards every inbound parcel to the outbound topic, through the sink it obtained by name from the
     * configuration.
     */
    public static class Forwarder {

        final List<String> received = new CopyOnWriteArrayList<>();
        private volatile EventSink kafka;

        Forwarder using(EventSink kafka) {
            this.kafka = kafka;
            return this;
        }

        @EventHandler
        public CompletableFuture<Void> on(ParcelDispatched event,
                                          MessageTypeResolver typeResolver,
                                          ProcessingContext context) {
            received.add(event.parcelId() + "@" + context.getResource(KafkaResources.TOPIC_PARTITION).topic());
            return EventSinks.publishPayloads(kafka, context, typeResolver, new ParcelForwarded(event.parcelId()));
        }
    }

    public static class Recorder {

        final List<String> received = new CopyOnWriteArrayList<>();

        @EventHandler
        public void on(ParcelDispatched event) {
            received.add(event.parcelId());
        }
    }

    @Nested
    class ProcessorsOnNamedComponents {

        @Test
        void pooledProcessorReadsNamedSourceAndPublishesThroughNamedSink() {
            // given
            Forwarder forwarder = new Forwarder();
            var processor = EventProcessorModule.pooledStreaming("forwarding")
                                                .eventHandlingComponents(c -> c.autodetected(
                                                        "forwarder",
                                                        cfg -> forwarder.using(
                                                                cfg.getComponent(EventSink.class, "outbound")
                                                        )
                                                ))
                                                .customized((cfg, c) -> c.eventSource(parcels(cfg))
                                                                         .tokenStore(new InMemoryTokenStore()));
            configuration = MessagingConfigurer.create()
                                               .componentRegistry(cr -> cr.registerEnhancer(
                                                       connector().sink("outbound", TopicResolver.fixed(outbound))
                                                                  .streamableSource("parcels",
                                                                                    TopicSubscription.topics(inbound))))
                                               .eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(processor)))
                                               .build();
            configuration.start();

            // when
            publishInbound(event(new ParcelDispatched("p-1")));

            // then
            await().atMost(Duration.ofSeconds(60)).until(() -> forwarder.received.size() == 1);
            assertThat(forwarder.received).containsExactly("p-1@" + inbound);
            await().atMost(Duration.ofSeconds(30)).until(
                    () -> KafkaTestBroker.readAll(outbound, "read_uncommitted", Duration.ofSeconds(1)).size() == 1
            );
        }

        @Test
        void subscribingProcessorConsumesNamedSourceOnceStarted() {
            // given
            Recorder recorder = new Recorder();
            var processor = EventProcessorModule.subscribing("billing")
                                                .eventHandlingComponents(
                                                        c -> c.autodetected("recorder", cfg -> recorder)
                                                )
                                                .customized((cfg, c) -> c.eventSource(
                                                        cfg.getComponent(SubscribableEventSource.class, "parcels")
                                                ));
            configuration = MessagingConfigurer.create()
                                               .componentRegistry(cr -> cr.registerEnhancer(
                                                       connector().subscribableSource("parcels",
                                                                                      TopicSubscription.topics(inbound),
                                                                                      inbound + "-group")))
                                               .eventProcessing(ep -> ep.subscribing(sp -> sp.processor(processor)))
                                               .build();
            configuration.start();

            // when
            publishInbound(event(new ParcelDispatched("p-1")));

            // then
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.received.size() == 1);
        }

        @Test
        void configuredTransformationsApplyToEventsFromKafka() {
            // given - an older producer still publishes the event under its former name
            MessageType legacyType = new MessageType("com.example.LegacyParcel", "1.0.0");
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.rename(
                                                                       legacyType,
                                                                       TYPES.resolveOrThrow(ParcelDispatched.class)))
                                                               .build();
            Recorder recorder = new Recorder();
            var processor = EventProcessorModule.pooledStreaming("transformed")
                                                .eventHandlingComponents(
                                                        c -> c.autodetected("recorder", cfg -> recorder)
                                                )
                                                .customized((cfg, c) -> c.eventSource(parcels(cfg))
                                                                         .tokenStore(new InMemoryTokenStore()));
            configuration = MessagingConfigurer.create()
                                               .componentRegistry(cr -> cr
                                                       .registerComponent(EventTransformerChain.class, cfg -> chain)
                                                       .registerEnhancer(connector().streamableSource(
                                                               "parcels", TopicSubscription.topics(inbound)
                                                       )))
                                               .eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(processor)))
                                               .build();
            configuration.start();

            // when
            byte[] payload = new JacksonConverter().convert(new ParcelDispatched("legacy-1"), byte[].class);
            publishInbound(new GenericEventMessage(legacyType, payload));

            // then
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.received.size() == 1);
            assertThat(recorder.received).containsExactly("legacy-1");
        }
    }

    @Nested
    class ExposingEvents {

        @Event(namespace = "shipping", name = "OrderShipped", version = "1.0")
        public record OrderShipped(String orderId) {

        }

        @Test
        void publishesOnlyTheConfiguredEventsFromTheEventStore() {
            // given
            configuration = EventSourcingConfigurer.create()
                                                   .componentRegistry(cr -> cr.registerEnhancer(
                                                           connector().sink("outbound", TopicResolver.fixed(outbound))
                                                                      .exposeEvents("parcel-exposure", "outbound",
                                                                                    ParcelDispatched.class)))
                                                   .build();
            configuration.start();
            // the exposure starts at the end of the store, so publish once it has claimed its segments
            PooledStreamingEventProcessor exposing = (PooledStreamingEventProcessor)
                    configuration.getComponents(EventProcessor.class).get("parcel-exposure");
            await().atMost(Duration.ofSeconds(30)).until(() -> !exposing.processingStatus().isEmpty());

            // when
            // published through the gateway, so that the application's resolver types the events
            configuration.getComponent(EventGateway.class)
                         .publish(List.of(new ParcelDispatched("exposed"), new OrderShipped("internal")))
                         .orTimeout(10, TimeUnit.SECONDS).join();

            // then
            await().atMost(Duration.ofSeconds(60)).until(
                    () -> !KafkaTestBroker.readAll(outbound, "read_uncommitted", Duration.ofSeconds(1)).isEmpty()
            );
            assertThat(KafkaTestBroker.readAll(outbound, "read_uncommitted", Duration.ofSeconds(2)))
                    .singleElement()
                    .satisfies(record -> {
                        EventMessage exposed = codec.decode(record).orElseThrow().event();
                        assertThat(exposed.type().name()).isEqualTo("parcels.ParcelDispatched");
                        assertThat(exposed.payloadAs(ParcelDispatched.class).parcelId()).isEqualTo("exposed");
                    });
        }
    }
}
