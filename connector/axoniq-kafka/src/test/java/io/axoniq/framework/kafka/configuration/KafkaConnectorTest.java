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
import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.codec.LegacyKafkaEventCodec;
import io.axoniq.framework.kafka.common.KafkaClientFactory;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.publishing.ConfirmationMode;
import io.axoniq.framework.kafka.publishing.EventSinks;
import io.axoniq.framework.kafka.publishing.TopicResolver;
import io.axoniq.framework.kafka.subscribing.KafkaSourceStatus;
import io.axoniq.framework.kafka.utils.RecordingKafkaClientFactory;
import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import io.axoniq.framework.messaging.transformation.events.TransformingSubscribableEventSource;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.annotation.AnnotationMessageTypeResolver;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.tracing.TracingEventSink;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.axonframework.messaging.eventstreaming.Tag;
import org.axonframework.messaging.tracing.LoggingSpanFactory;
import org.axonframework.messaging.tracing.SpanFactory;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class KafkaConnectorTest {

    private static final String TOPIC = "parcels";

    private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(true);
    private @Nullable AxonConfiguration configuration;

    @Event(namespace = "parcels", name = "ParcelDispatched", version = "1.0")
    public record ParcelDispatched(String parcelId) {

    }

    @Event(namespace = "shipping", name = "OrderShipped", version = "1.0")
    public record OrderShipped(String orderId) {

    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    private static KafkaConnector connector() {
        return KafkaConnector.usingRegisteredClientFactory();
    }

    @Nested
    class Naming {

        @Test
        void connectorRejectsDuplicateNames() {
            // when / then
            assertThatThrownBy(() -> connector().sink("parcels", TopicResolver.fixed(TOPIC))
                                                .streamableSource("parcels", TopicSubscription.topics(TOPIC)))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void secondConnectorIsRejected() {
            // given - a second connector that reaches the registry, as a second connector bean does in Spring
            KafkaConnector second = connector().sink("b", TopicResolver.fixed(TOPIC));

            // when / then - the second connector's sinks and sources would use the first one's client factory
            assertThatThrownBy(() -> MessagingConfigurer.create()
                                                        .componentRegistry(cr -> cr.registerEnhancer(
                                                                connector().sink("a", TopicResolver.fixed(TOPIC))
                                                        ).registerEnhancer(second::enhance))
                                                        .build())
                    .isInstanceOf(AxonConfigurationException.class)
                    .hasMessageContaining("KafkaConnector");
        }

        @Test
        void exposureNeedsAConfiguredSink() {
            // when / then
            assertThatThrownBy(() -> EventSourcingConfigurer.create()
                                                            .componentRegistry(cr -> cr.registerEnhancer(
                                                                    connector().exposeEvents(
                                                                            "parcel-exposure", "missing",
                                                                            ParcelDispatched.class)))
                                                            .build())
                    .hasMessageContaining("missing");
        }
    }

    private void awaitExposureProcessorRunning() {
        // the exposure starts at the end of the store, so publish once it has claimed its segments
        PooledStreamingEventProcessor processor = (PooledStreamingEventProcessor)
                configuration.getComponents(EventProcessor.class).get("parcel-exposure");
        await().atMost(Duration.ofSeconds(10)).until(() -> !processor.processingStatus().isEmpty());
    }

    @Nested
    class WithTracing {

        @Test
        void namedSinkIsResolvableAndPublishesWhenTracingIsConfigured() {
            // given
            configuration = MessagingConfigurer.create()
                                               .componentRegistry(cr -> cr
                                                       .registerComponent(SpanFactory.class,
                                                                          c -> LoggingSpanFactory.INSTANCE)
                                                       .registerComponent(KafkaClientFactory.class,
                                                                          c -> clientFactory)
                                                       .registerEnhancer(connector().sink(
                                                               "outbound", TopicResolver.fixed(TOPIC))))
                                               .build();
            configuration.start();

            // when
            EventSink sink = configuration.getComponent(EventSink.class, "outbound");
            EventSinks.publishPayloads(sink, null, configuration.getComponent(MessageTypeResolver.class),
                                       new ParcelDispatched("p-1"))
                      .orTimeout(10, TimeUnit.SECONDS).join();

            // then
            assertThat(sink).isInstanceOf(TracingEventSink.class);
            assertThat(clientFactory.<byte[]>producers().getFirst().history())
                    .singleElement().extracting(ProducerRecord::topic).isEqualTo(TOPIC);
        }

        @Test
        void decoratedSinkClosesItsProducersAtShutdown() {
            // given
            configuration = MessagingConfigurer.create()
                                               .componentRegistry(cr -> cr
                                                       .registerComponent(SpanFactory.class,
                                                                          c -> LoggingSpanFactory.INSTANCE)
                                                       .registerComponent(KafkaClientFactory.class,
                                                                          c -> clientFactory)
                                                       .registerEnhancer(connector().sink(
                                                               "outbound", TopicResolver.fixed(TOPIC))))
                                               .build();
            configuration.start();
            EventSink sink = configuration.getComponent(EventSink.class, "outbound");
            EventSinks.publishPayloads(sink, null, configuration.getComponent(MessageTypeResolver.class),
                                       new ParcelDispatched("p-1"))
                      .orTimeout(10, TimeUnit.SECONDS).join();

            // when
            configuration.shutdown();

            // then
            assertThat(sink).isInstanceOf(TracingEventSink.class);
            await().atMost(Duration.ofSeconds(10)).until(() -> clientFactory.producers().getFirst().closed());
        }

        @Test
        void unnamedEventSinkStillResolvesToTheApplicationsOwn() {
            // given
            configuration = MessagingConfigurer.create()
                                               .componentRegistry(cr -> cr
                                                       .registerComponent(SpanFactory.class,
                                                                          c -> LoggingSpanFactory.INSTANCE)
                                                       .registerComponent(KafkaClientFactory.class,
                                                                          c -> clientFactory)
                                                       .registerEnhancer(connector().sink(
                                                               "outbound", TopicResolver.fixed(TOPIC))))
                                               .build();
            configuration.start();

            // when
            EventSink unnamed = configuration.getComponent(EventSink.class);
            ParcelDispatched payload = new ParcelDispatched("internal");
            unnamed.publish(null, new GenericEventMessage(new AnnotationMessageTypeResolver().resolveOrThrow(payload),
                                                          payload))
                   .orTimeout(10, TimeUnit.SECONDS).join();

            // then
            assertThat(unnamed).isNotSameAs(configuration.getComponent(EventSink.class, "outbound"));
            assertThat(clientFactory.producers()).isEmpty();
        }

        @Test
        void exposureProcessorPublishesWhenTracingIsConfigured() {
            // given
            configuration = EventSourcingConfigurer.create()
                                                   .componentRegistry(cr -> cr
                                                           .registerComponent(SpanFactory.class,
                                                                              c -> LoggingSpanFactory.INSTANCE)
                                                           .registerComponent(KafkaClientFactory.class,
                                                                              c -> clientFactory)
                                                           .registerEnhancer(
                                                                   connector().sink("outbound",
                                                                                    TopicResolver.fixed(TOPIC))
                                                                              .exposeEvents(
                                                                                      "parcel-exposure", "outbound",
                                                                                      ParcelDispatched.class)))
                                                   .build();
            configuration.start();
            awaitExposureProcessorRunning();

            // when
            configuration.getComponent(EventGateway.class)
                         .publish(List.of(new ParcelDispatched("exposed"), new OrderShipped("internal")))
                         .orTimeout(10, TimeUnit.SECONDS).join();

            // then
            await().atMost(Duration.ofSeconds(10)).until(
                    () -> !clientFactory.producers().isEmpty()
                            && !clientFactory.producers().getFirst().history().isEmpty()
            );
            assertThat(clientFactory.<byte[]>producers().getFirst().history())
                    .singleElement().extracting(ProducerRecord::topic).isEqualTo(TOPIC);
        }
    }

    /**
     * Returns the properties the given {@code component} describes, looking through decorators.
     */
    private static Map<String, Object> describedProperties(Object component) {
        MockComponentDescriptor descriptor = new MockComponentDescriptor();
        ((DescribableComponent) component).describeTo(descriptor);
        Object delegate = descriptor.getDescribedProperties().get("delegate");
        return delegate instanceof DescribableComponent ? describedProperties(delegate)
                : descriptor.getDescribedProperties();
    }

    private AxonConfiguration configure(KafkaConnector connector, Consumer<ComponentRegistry> registrations) {
        configuration = MessagingConfigurer.create()
                                           .componentRegistry(cr -> {
                                               cr.registerComponent(KafkaClientFactory.class, c -> clientFactory)
                                                 .registerEnhancer(connector);
                                               registrations.accept(cr);
                                           })
                                           .build();
        return configuration;
    }

    @Nested
    class Codecs {

        @Test
        void defaultCodecWritesTheTagsOfTheApplicationsTagResolver() {
            // given
            TagResolver tagResolver = event -> Set.of(Tag.of("parcelId", "p-1"));

            // when
            AxonConfiguration config = configure(connector(),
                                                 cr -> cr.registerComponent(TagResolver.class, c -> tagResolver));

            // then
            KafkaEventCodec<?> codec = config.getComponent(KafkaEventCodec.class);
            assertThat(codec).isInstanceOf(DefaultKafkaEventCodec.class);
            assertThat(describedProperties(codec)).containsEntry("tagResolver", tagResolver);
        }

        @Test
        void legacyFormatWrapsTheDefaultCodec() {
            // when
            AxonConfiguration config = configure(connector().withLegacyFormat(), cr -> {
            });

            // then
            assertThat(config.getComponent(KafkaEventCodec.class)).isInstanceOf(LegacyKafkaEventCodec.class);
        }

        @Test
        void applicationRegisteredCodecWins() {
            // given
            KafkaEventCodec<byte[]> own = CodecTestFixtures.defaultCodec();

            // when
            AxonConfiguration config = configure(
                    connector().withLegacyFormat().sink("outbound", TopicResolver.fixed(TOPIC)),
                    cr -> cr.registerComponent(KafkaEventCodec.class, c -> own)
            );

            // then
            assertThat(config.getComponent(KafkaEventCodec.class)).isSameAs(own);
            assertThat(describedProperties(config.getComponent(EventSink.class, "outbound")))
                    .containsEntry("codec", own);
        }
    }

    @Nested
    class Customizations {

        @Test
        void customizationsAdjustTheSettingsOfSinksAndSources() {
            // when
            AxonConfiguration config = configure(
                    connector().sink("outbound", TopicResolver.fixed(TOPIC),
                                     (c, sink) -> sink.withConfirmationMode(ConfirmationMode.NONE))
                               .streamableSource("stream", TopicSubscription.topics(TOPIC),
                                                 (c, source) -> source.withBufferSize(7))
                               .subscribableSource("group", TopicSubscription.topics(TOPIC), "billing",
                                                   (c, source) -> source.withConsumerCount(2)),
                    cr -> {
                    }
            );

            // then
            assertThat(describedProperties(config.getComponent(EventSink.class, "outbound")))
                    .containsEntry("confirmationMode", ConfirmationMode.NONE);
            assertThat(describedProperties(config.getComponent(StreamableEventSource.class, "stream")))
                    .containsEntry("bufferSize", 7);
            assertThat(describedProperties(config.getComponent(SubscribableEventSource.class, "group")))
                    .containsEntry("consumerCount", 2);
        }

        @Test
        void sinkTopicResolverCanBeBuiltFromTheConfiguration() {
            // given
            TopicResolver registered = TopicResolver.fixed("registered");

            // when
            AxonConfiguration config = configure(
                    connector().sink("outbound", c -> c.getComponent(TopicResolver.class), (c, sink) -> sink),
                    cr -> cr.registerComponent(TopicResolver.class, c -> registered)
            );

            // then
            assertThat(describedProperties(config.getComponent(EventSink.class, "outbound")))
                    .containsEntry("topicResolver", registered);
        }

        @Test
        void configuredTransformationsApplyToTheSubscribableSource() {
            // given
            EventTransformerChain chain = EventTransformerChain.builder().build();

            // when
            AxonConfiguration config = configure(
                    connector().subscribableSource("group", TopicSubscription.topics(TOPIC), "billing"),
                    cr -> cr.registerComponent(EventTransformerChain.class, c -> chain)
            );

            // then
            assertThat(config.getComponent(SubscribableEventSource.class, "group"))
                    .isInstanceOf(TransformingSubscribableEventSource.class);
        }
    }

    @Nested
    class Status {

        /**
         * Handles nothing; it only gives the subscribing processor a handler, so that it subscribes to its source.
         */
        public static class ParcelListener {

            @EventHandler
            public void on(ParcelDispatched event) {
            }
        }

        private AxonConfiguration startWithSubscribingProcessor(Consumer<ComponentRegistry> registrations) {
            var processor = EventProcessorModule.subscribing("billing")
                                                .eventHandlingComponents(
                                                        c -> c.autodetected("listener", cfg -> new ParcelListener())
                                                )
                                                .customized((cfg, c) -> c.eventSource(
                                                        cfg.getComponent(SubscribableEventSource.class, "group")
                                                ));
            configuration = MessagingConfigurer.create()
                                               .componentRegistry(cr -> {
                                                   cr.registerComponent(KafkaClientFactory.class, c -> clientFactory)
                                                     .registerEnhancer(connector().subscribableSource(
                                                             "group", TopicSubscription.topics(TOPIC), "billing"
                                                     ).subscribableSource(
                                                             "idle", TopicSubscription.topics(TOPIC), "auditing"
                                                     ));
                                                   registrations.accept(cr);
                                               })
                                               .eventProcessing(ep -> ep.subscribing(sp -> sp.processor(processor)))
                                               .build();
            configuration.start();
            return configuration;
        }

        @Test
        void reportsTheStatusOfEverySubscribableSource() {
            // when
            AxonConfiguration config = startWithSubscribingProcessor(cr -> {
            });

            // then - the source of the processor runs; the other one is not used
            KafkaConnectorStatus status = config.getComponent(KafkaConnectorStatus.class);
            await().atMost(Duration.ofSeconds(5)).until(() -> !clientFactory.consumerProperties().isEmpty());
            assertThat(status.sources()).containsOnlyKeys("group", "idle");
            assertThat(status.source("group")).containsInstanceOf(KafkaSourceStatus.Running.class);
            assertThat(status.source("idle")).contains(new KafkaSourceStatus.Stopped());
            assertThat(status.source("unknown")).isEmpty();
            assertThat(status.healthy()).isTrue();
        }

        @Test
        void sourceCreatesItsClientsUnderItsName() {
            // when
            startWithSubscribingProcessor(cr -> {
            });

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> !clientFactory.consumerProperties().isEmpty());
            assertThat(clientFactory.componentNames()).contains("group");
        }

        @Test
        void reportsAFailedSourceAlsoWhenATransformationDecoratesIt() {
            // given
            clientFactory.failConsumerCreationWith(
                    new KafkaException("Failed to construct kafka consumer", new ConfigException("bad settings"))
            );
            EventTransformerChain chain = EventTransformerChain.builder().build();

            // when
            AxonConfiguration config = startWithSubscribingProcessor(
                    cr -> cr.registerComponent(EventTransformerChain.class, c -> chain)
            );

            // then
            KafkaConnectorStatus status = config.getComponent(KafkaConnectorStatus.class);
            assertThat(config.getComponent(SubscribableEventSource.class, "group"))
                    .isInstanceOf(TransformingSubscribableEventSource.class);
            await().atMost(Duration.ofSeconds(5)).until(() -> !status.healthy());
            assertThat(status.source("group")).hasValueSatisfying(
                    source -> assertThat(source).isInstanceOfSatisfying(
                            KafkaSourceStatus.Failed.class,
                            failed -> assertThat(failed.cause()).hasRootCauseInstanceOf(ConfigException.class)
                    )
            );
        }
    }

    @Nested
    class ExposureDefinitions {

        @Test
        void exposesEventsNamedByQualifiedName() {
            // given
            KafkaConnector connector = connector().sink("outbound", TopicResolver.fixed(TOPIC))
                                                  .exposeEvents("parcel-exposure", "outbound",
                                                                Set.of(new QualifiedName("parcels.ParcelDispatched")));

            // when
            configuration = EventSourcingConfigurer.create()
                                                   .componentRegistry(cr -> cr.registerComponent(
                                                           KafkaClientFactory.class, c -> clientFactory
                                                   ).registerEnhancer(connector))
                                                   .build();

            // then
            assertThat(configuration.getComponents(EventProcessor.class)).containsKey("parcel-exposure");
        }

        @Test
        void rejectsExposingNoEvents() {
            // when / then
            assertThatThrownBy(() -> connector().exposeEvents("parcel-exposure", "outbound", Set.of()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> connector().exposeEvents("parcel-exposure", "outbound",
                                                              Set.<QualifiedName>of(), (c, processor) -> processor))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> connector().exposeEvents("parcel-exposure", "outbound"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> connector().exposeEvents("parcel-exposure", "outbound",
                                                              (c, processor) -> processor))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class Exposure {

        private final InMemoryEventStorageEngine storageEngine = new InMemoryEventStorageEngine();

        private void storeHistory() {
            AxonConfiguration history = EventSourcingConfigurer.create()
                                                               .componentRegistry(cr -> cr.registerComponent(
                                                                       EventStorageEngine.class,
                                                                       c -> storageEngine))
                                                               .build();
            history.start();
            history.getComponent(EventGateway.class)
                   .publish(List.of(new ParcelDispatched("history")))
                   .orTimeout(10, TimeUnit.SECONDS).join();
            history.shutdown();
        }

        private void startExposing(KafkaConnector connector) {
            configuration = EventSourcingConfigurer.create()
                                                   .componentRegistry(cr -> cr
                                                           .registerComponent(EventStorageEngine.class,
                                                                              c -> storageEngine)
                                                           .registerComponent(KafkaClientFactory.class,
                                                                              c -> clientFactory)
                                                           .registerEnhancer(connector))
                                                   .build();
            configuration.start();
            PooledStreamingEventProcessor processor = (PooledStreamingEventProcessor)
                    configuration.getComponents(EventProcessor.class).get("parcel-exposure");
            await().atMost(Duration.ofSeconds(10)).until(() -> !processor.processingStatus().isEmpty());
        }

        private List<String> publishedParcels() {
            if (clientFactory.producers().isEmpty()) {
                return List.of();
            }
            return clientFactory.<byte[]>producers().getFirst().history().stream()
                                .map(record -> new String(record.value(), StandardCharsets.UTF_8))
                                .toList();
        }

        @Test
        void exposureStartsAfterTheEventsStoredBefore() {
            // given
            storeHistory();
            startExposing(connector().sink("outbound", TopicResolver.fixed(TOPIC))
                                     .exposeEvents("parcel-exposure", "outbound", ParcelDispatched.class));

            // when
            configuration.getComponent(EventGateway.class)
                         .publish(List.of(new ParcelDispatched("new")))
                         .orTimeout(10, TimeUnit.SECONDS).join();

            // then
            await().atMost(Duration.ofSeconds(10)).until(() -> publishedParcels().size() == 1);
            await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5))
                   .until(() -> publishedParcels().size() == 1);
            assertThat(publishedParcels()).singleElement().asString().contains("new");
        }

        @Test
        void exposureProcessorCannotBeReset() {
            // given
            startExposing(connector().sink("outbound", TopicResolver.fixed(TOPIC))
                                     .exposeEvents("parcel-exposure", "outbound", ParcelDispatched.class));
            PooledStreamingEventProcessor processor = (PooledStreamingEventProcessor)
                    configuration.getComponents(EventProcessor.class).get("parcel-exposure");

            // when
            CompletableFuture<Void> reset = processor.shutdown()
                                                     .thenCompose(stopped -> processor.resetTokens());

            // then
            assertThat(processor.supportsReset()).isFalse();
            assertThat(reset).failsWithin(Duration.ofSeconds(10));
        }

        @Test
        void customizationCanStartTheExposureAtTheBeginning() {
            // given
            storeHistory();

            // when
            startExposing(connector().sink("outbound", TopicResolver.fixed(TOPIC))
                                     .exposeEvents("parcel-exposure", "outbound",
                                                   (config, processor) -> processor.initialToken(
                                                           source -> source.firstToken(null)),
                                                   ParcelDispatched.class));

            // then
            await().atMost(Duration.ofSeconds(10)).until(() -> publishedParcels().size() == 1);
            assertThat(publishedParcels()).singleElement().asString().contains("history");
        }
    }
}
