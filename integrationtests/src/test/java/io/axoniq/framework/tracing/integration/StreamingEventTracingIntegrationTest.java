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

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.configuration.MessagingTracingSettings;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies the default streaming trace topology through a pooled streaming processor: the batch span parents the
 * per-event span and both spans identify their processor.
 */
class StreamingEventTracingIntegrationTest {

    private static final String BATCH_SPAN = "StreamingEventProcessor.batch";
    private static final String PROCESS_SPAN_PREFIX = "EventProcessor.process";
    private static final String PROCESSOR_NAME = "streaming-tracing-test-processor";
    private static final AttributeKey<String> PROCESSOR_NAME_ATTRIBUTE =
            AttributeKey.stringKey("axoniq.event_processor.name");

    private MicrometerTracingTestSetup tracing;
    private InMemorySpanExporter spanExporter;
    private SpanFactory spanFactory;
    private @Nullable AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        tracing = MicrometerTracingTestSetup.create();
        spanExporter = tracing.spanExporter();
        spanFactory = tracing.spanFactory();
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
        tracing.close();
    }

    @Test
    void streamingBatchParentsEventSpanAndCarriesProcessorAttributes() {
        // given
        AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();
        BookingProjection projection = new BookingProjection();
        configuration = startApplication(eventSource, projection);

        // when
        EventMessage event = EventTestUtils.asEventMessage("room-42");
        eventSource.publishMessage(event);

        // then
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(projection.handledEvent.get()).isEqualTo("room-42"));
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(spanNames()).anyMatch(name -> name.startsWith(PROCESS_SPAN_PREFIX)));

        SpanData batchSpan = spanNamed(BATCH_SPAN);
        SpanData processSpan = spanStartingWith(PROCESS_SPAN_PREFIX);
        assertThat(processSpan.getParentSpanId()).isEqualTo(batchSpan.getSpanId());
        assertThat(processSpan.getTraceId()).isEqualTo(batchSpan.getTraceId());
        assertThat(batchSpan.getAttributes().get(PROCESSOR_NAME_ATTRIBUTE)).isEqualTo(PROCESSOR_NAME);
        assertThat(processSpan.getAttributes().get(PROCESSOR_NAME_ATTRIBUTE)).isEqualTo(PROCESSOR_NAME);
    }

    private AxonConfiguration startApplication(AsyncInMemoryStreamableEventSource eventSource,
                                               BookingProjection projection) {
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> registry
                                          .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                          .registerComponent(SpanFactory.class, c -> spanFactory)
                                          .registerComponent(
                                                  MessagingTracingSettings.class,
                                                  c -> MessagingTracingSettings.enabledByDefault()
                                          )
                                          .registerComponent(Tracer.class, c -> tracing.tracer()))
                                  .eventProcessing(eventProcessing -> eventProcessing.pooledStreaming(
                                          pooledStreaming -> pooledStreaming.defaults(
                                                                                   defaults -> defaults.eventSource(
                                                                                           eventSource
                                                                                   )
                                                                           )
                                                                           .defaultProcessor(
                                                                                   PROCESSOR_NAME,
                                                                                   component -> component.autodetected(
                                                                                           "bookings",
                                                                                           c -> projection
                                                                                   )
                                                                           )
                                  ))
                                  .start();
    }

    private List<String> spanNames() {
        return spanExporter.getFinishedSpanItems().stream().map(SpanData::getName).toList();
    }

    private SpanData spanNamed(String name) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().equals(name))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError("No span named " + name + " in " + spanNames()));
    }

    private SpanData spanStartingWith(String prefix) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().startsWith(prefix))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError(
                                   "No span starting with " + prefix + " in " + spanNames()
                           ));
    }

    @SuppressWarnings("unused")
    static class BookingProjection {

        private final AtomicReference<String> handledEvent = new AtomicReference<>();

        @EventHandler
        public void on(String event) {
            handledEvent.set(event);
        }
    }
}
