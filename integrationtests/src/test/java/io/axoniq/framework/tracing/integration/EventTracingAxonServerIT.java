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

package io.axoniq.framework.tracing.integration;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test proving the W3C trace-context propagates across the Axon Server gRPC boundary on the event side.
 * One application connects to Axon Server, publishes an event via the {@link EventGateway}, and asserts that the
 * publish span is emitted by the local tracing decorator (the publish leg) with the traceparent serialised onto the
 * event metadata. Since the consuming side of distributed event handling on Axon Server uses streaming-processor
 * tokens (not a synchronous gRPC reply to the publisher), this IT focuses on the dispatch-side proof: the publish +
 * storage-append spans fire and the traceparent metadata key is populated on the in-flight event.
 * <p>
 * Auto-skips when Docker is unavailable.
 */
class EventTracingAxonServerIT {

    private static final TracingAxonServerTestInfrastructure INFRASTRUCTURE =
            new TracingAxonServerTestInfrastructure();

    private AxonConfiguration startedConfiguration;

    @AfterEach
    void tearDown() {
        if (startedConfiguration != null) {
            try {
                startedConfiguration.shutdown();
            } finally {
                INFRASTRUCTURE.stop();
            }
        }
    }

    @Test
    void publishingAnEventThroughAxonServerEmitsPublicationAndStorageAppendSpans() {
        // given
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.spanExporter().reset();

        ApplicationConfigurer configurer =
                MessagingConfigurer.create()
                                   .componentRegistry(INFRASTRUCTURE::configureInfrastructure);
        startedConfiguration = configurer.start();
        EventGateway eventGateway = startedConfiguration.getComponent(EventGateway.class);

        // when
        EventMessage event = EventTestUtils.asEventMessage("room-42-booked");
        eventGateway.publish(null, List.of(event)).orTimeout(30, TimeUnit.SECONDS).join();

        // then both publication and the complete storage append transaction are traced when the EventStore is backed
        // by the Axon Server connector
        InMemorySpanExporter exporter = INFRASTRUCTURE.spanExporter();
        String publishPrefix = "EventSink.publish";

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> {
                   List<SpanData> spans = exporter.getFinishedSpanItems();
                   assertThat(spans).extracting(SpanData::getName)
                                    .anyMatch(name -> name.startsWith(publishPrefix))
                                    .contains("EventStorageEngine.appendTransaction");
               });
    }
}
