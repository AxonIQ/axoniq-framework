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
import io.axoniq.framework.tracing.micrometer.MicrometerTracingConfigurationEnhancer;
import io.axoniq.framework.tracing.micrometer.threadlocal.MicrometerThreadLocalContextPropagationConfigurationEnhancer;
import org.axonframework.messaging.tracing.SpanFactory;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
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
 * Integration test for event-side tracing through the real configuration: a {@link MessagingConfigurer} registers an
 * a Micrometer {@code SpanFactory} as a component, disables the Axon Server connector enhancer to stay local, and
 * wires a {@link SimpleEventBus}-backed subscribing event processor against an annotated {@code @EventHandler}.
 * Publishing through the event bus must trigger the per-method enhancer span
 * {@code BookingProjection.on(String)} (INTERNAL), proving the ServiceLoader-discovered tracing handler enhancer
 * fires inside the annotation pipeline for events just as it does for commands — no manual decorator / handler-
 * definition wiring (per constitution §Testing). With no {@link SpanFactory} registered, the same wiring produces no
 * spans.
 */
class EventTracingConfigurationIntegrationTest {

    private static final String ENHANCER_SPAN = "BookingProjection.on(String)";

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
    void anAnnotatedEventHandlerProducesThePerMethodEnhancerSpan() {
        // given a subscribing processor wired around an annotated @EventHandler through the real configuration
        SimpleEventBus eventBus = new SimpleEventBus();
        BookingProjection projection = new BookingProjection();
        configuration = startApplication(eventBus, projection, /* registerSpanFactory */ true);

        // when an event is published through the configured event bus
        EventMessage event = EventTestUtils.asEventMessage("room-42");
        FutureUtils.joinAndUnwrap(eventBus.publish(null, event));

        // then the annotated handler ran and the per-method enhancer span was produced
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(projection.handledEvent.get()).isEqualTo("room-42"));
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(spanNames()).contains(ENHANCER_SPAN));
        SpanData enhancerSpan = spanNamed(ENHANCER_SPAN);
        assertThat(enhancerSpan.getKind()).isEqualTo(SpanKind.INTERNAL);
    }

    @Test
    void withoutAConfiguredSpanFactoryNoTracingSpansAreProduced() {
        // given the same wiring but with no SpanFactory registered — tracing degrades to a pass-through
        SimpleEventBus eventBus = new SimpleEventBus();
        BookingProjection projection = new BookingProjection();
        configuration = startApplication(eventBus, projection, /* registerSpanFactory */ false);

        // when
        EventMessage event = EventTestUtils.asEventMessage("room-42");
        FutureUtils.joinAndUnwrap(eventBus.publish(null, event));

        // then the handler still runs but nothing is exported
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(projection.handledEvent.get()).isEqualTo("room-42"));
        assertThat(spanExporter.getFinishedSpanItems()).isEmpty();
    }

    private AxonConfiguration startApplication(SimpleEventBus eventBus,
                                               BookingProjection projection,
                                               boolean registerSpanFactory) {
        MessagingConfigurer configurer = MessagingConfigurer.create()
                                                            .componentRegistry(registry -> registry
                                                                    // Stay local: the Axon Server connector would
                                                                    // otherwise register a CommandBusConnector and
                                                                    // flip the bus to DistributedCommandBus.
                                                                    .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                                            )
                                                            .eventProcessing(ep -> ep.subscribing(
                                                                    sp -> sp.defaults(d -> d.eventSource(eventBus))
                                                                            .defaultProcessor(
                                                                                    "tracing-test-processor",
                                                                                    c -> c.autodetected("bookings",
                                                                                                        cfg -> projection)
                                                                            )
                                                            ));
        if (registerSpanFactory) {
            configurer.componentRegistry(registry -> registry
                    .registerComponent(SpanFactory.class, c -> spanFactory)
                    .registerComponent(Tracer.class, c -> tracing.tracer()));
        } else {
            // No bridge components available in this scenario -- disable the Micrometer enhancers so they don't
            // fail requiring a Tracer to build their own SpanFactory / thread-local bridge.
            configurer.componentRegistry(registry -> registry
                    .disableEnhancer(MicrometerTracingConfigurationEnhancer.class)
                    .disableEnhancer(MicrometerThreadLocalContextPropagationConfigurationEnhancer.class));
        }
        return configurer.start();
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

    /**
     * Annotated event handler. Its {@code @EventHandler} method is what the tracing enhancer must wrap to produce
     * the per-method span {@code BookingProjection.on(String)}.
     */
    @SuppressWarnings("unused")
    static class BookingProjection {

        private final AtomicReference<String> handledEvent = new AtomicReference<>();

        @EventHandler
        public void on(String event) {
            handledEvent.set(event);
        }
    }
}
