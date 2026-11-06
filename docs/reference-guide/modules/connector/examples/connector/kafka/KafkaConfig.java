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

package connector.kafka;

import io.axoniq.framework.kafka.codec.CloudEventKafkaEventCodec;
import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.configuration.KafkaConnector;
import io.axoniq.framework.kafka.publishing.ConfirmationMode;
import io.axoniq.framework.kafka.publishing.RecordKeyResolver;
import io.axoniq.framework.kafka.publishing.TopicResolver;
import io.axoniq.framework.kafka.subscribing.FailedRecordPolicy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;

import java.net.URI;
import java.util.Map;

/**
 * The Configuration API samples of the Kafka connector page.
 */
public class KafkaConfig {

    public AxonConfiguration configure() {
        // tag::connector[]
        KafkaConnector kafka =
                KafkaConnector.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))
                              .sink("outbound", TopicResolver.fixed("parcels"), // <1>
                                    (config, sink) -> sink
                                            .withRecordKeyResolver(RecordKeyResolver.fromMetadata("parcelId"))
                                            .withConfirmationMode(ConfirmationMode.TRANSACTIONAL)
                                            .withTransactionalIdPrefix("shipping-1-"))
                              .streamableSource("parcel-stream", TopicSubscription.topics("parcels")) // <2>
                              .subscribableSource("parcel-group", TopicSubscription.topics("parcels"), "billing", // <3>
                                                  (config, source) -> source
                                                          .withConsumerCount(4)
                                                          .withFailedRecordPolicy(
                                                                  FailedRecordPolicy.DEAD_LETTER_TOPIC
                                                          ));
        // end::connector[]

        // tag::processors[]
        EventSourcingConfigurer configurer = EventSourcingConfigurer.create();
        configurer.componentRegistry(registry -> registry.registerEnhancer(kafka))
                  .messaging(messaging -> messaging.eventProcessing(processing -> processing
                          .pooledStreaming(pooled -> pooled.processor( // <1>
                                  EventProcessorModule.pooledStreaming("parcel-projection")
                                                      .eventHandlingComponents(components -> components.autodetected(
                                                              "parcel-projection", config -> new ParcelProjection()))
                                                      .customized((config, processor) -> processor
                                                              .eventSource(config.getComponent(
                                                                      StreamableEventSource.class, "parcel-stream"))
                                                              .initialSegmentCount(4))
                          ))
                          .subscribing(subscribing -> subscribing.processor( // <2>
                                  EventProcessorModule.subscribing("billing")
                                                      .eventHandlingComponents(components -> components.autodetected(
                                                              "billing", config -> new ParcelProjection()))
                                                      .customized((config, processor) -> processor
                                                              .eventSource(config.getComponent(
                                                                      SubscribableEventSource.class, "parcel-group")))
                          ))
                          .pooledStreaming(pooled -> pooled.processor( // <3>
                                  EventProcessorModule.pooledStreaming("parcel-publisher")
                                                      .eventHandlingComponents(components -> components.autodetected(
                                                              "parcel-publisher",
                                                              config -> new ParcelPublisher(config.getComponent(
                                                                      EventSink.class, "outbound"))))
                                                      .notCustomized()
                          ))
                  ));
        // end::processors[]
        return configurer.build();
    }

    public KafkaConnector exposeEvents() {
        // tag::expose[]
        return KafkaConnector.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))
                             .sink("outbound", TopicResolver.fixed("parcels"))
                             .exposeEvents("parcel-exposure", "outbound", ParcelEvents.ParcelDispatched.class);
        // end::expose[]
    }

    public KafkaConnector legacyFormat() {
        // tag::legacy[]
        return KafkaConnector.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))
                             .withLegacyFormat()
                             .streamableSource("parcel-stream", TopicSubscription.topics("parcels"));
        // end::legacy[]
    }

    public void cloudEvents(EventSourcingConfigurer configurer) {
        // tag::cloudevents[]
        configurer.componentRegistry(registry -> registry.registerComponent(
                KafkaEventCodec.class,
                config -> new CloudEventKafkaEventCodec(config.getComponent(EventConverter.class),
                                                       URI.create("/shipping"))
        ));
        // end::cloudevents[]
    }
}
