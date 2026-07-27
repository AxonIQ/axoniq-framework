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
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.configuration.MessagingTracingSettings;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
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
 * Integration test for the {@code @EventSourcingHandler} suppression of the per-method tracing handler enhancer,
 * through the real configuration: a {@link MessagingConfigurer} registers a Micrometer {@code SpanFactory}, the
 * ServiceLoader-discovered enhancer wraps a component whose handler method carries the <em>real</em>
 * {@link EventSourcingHandler} annotation, and the {@link MessagingTracingSettings} lookup goes through the real
 * component registry (no stubs). By default the invocation produces <b>no</b> per-method span — event sourcing
 * handlers fire once per event during entity replay and would flood traces — while registering settings with
 * {@code showEventSourcingHandlers=true} (the {@code axon.tracing.show-event-sourcing-handlers} property in Spring
 * Boot) restores the span.
 */
class EventSourcingHandlerTracingConfigurationIntegrationTest {

    private static final String ENHANCER_SPAN = "BookingState.on(String)";

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
    void anEventSourcingHandlerProducesNoPerMethodSpanByDefault() {
        // given the real wiring with a SpanFactory but default settings — showEventSourcingHandlers=false
        SimpleEventBus eventBus = new SimpleEventBus();
        BookingState state = new BookingState();
        configuration = startApplication(eventBus, state, /* showEventSourcingHandlers */ null);

        // when an event reaches the @EventSourcingHandler through the configured processor
        EventMessage event = EventTestUtils.asEventMessage("room-42");
        FutureUtils.joinAndUnwrap(eventBus.publish(null, event));

        // then the handler evolved the state, but the per-method enhancer span is suppressed
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(state.handledEvent.get()).isEqualTo("room-42"));
        assertThat(spanNames()).doesNotContain(ENHANCER_SPAN);
    }

    @Test
    void anEventSourcingHandlerProducesThePerMethodSpanWhenShowEventSourcingHandlersIsEnabled() {
        // given the same wiring with MessagingTracingSettings enabling showEventSourcingHandlers
        SimpleEventBus eventBus = new SimpleEventBus();
        BookingState state = new BookingState();
        configuration = startApplication(eventBus, state, /* showEventSourcingHandlers */ true);

        // when
        EventMessage event = EventTestUtils.asEventMessage("room-42");
        FutureUtils.joinAndUnwrap(eventBus.publish(null, event));

        // then the per-method enhancer span is produced for the @EventSourcingHandler invocation
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(state.handledEvent.get()).isEqualTo("room-42"));
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(spanNames()).contains(ENHANCER_SPAN));
        SpanData enhancerSpan = spanNamed(ENHANCER_SPAN);
        assertThat(enhancerSpan.getKind()).isEqualTo(SpanKind.INTERNAL);
    }

    private AxonConfiguration startApplication(SimpleEventBus eventBus,
                                               BookingState state,
                                               @Nullable Boolean showEventSourcingHandlers) {
        MessagingConfigurer configurer = MessagingConfigurer.create()
                                                            .componentRegistry(registry -> registry
                                                                    // Stay local: the Axon Server connector would
                                                                    // otherwise register a CommandBusConnector and
                                                                    // flip the bus to DistributedCommandBus.
                                                                    .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                                                    .registerComponent(SpanFactory.class,
                                                                                       c -> spanFactory)
                                                                    .registerComponent(Tracer.class,
                                                                                       c -> tracing.tracer())
                                                            )
                                                            .eventProcessing(ep -> ep.subscribing(
                                                                    sp -> sp.defaults(d -> d.eventSource(eventBus))
                                                                            .defaultProcessor(
                                                                                    "tracing-test-processor",
                                                                                    c -> c.autodetected("bookings",
                                                                                                        cfg -> state)
                                                                            )
                                                            ));
        if (showEventSourcingHandlers != null) {
            MessagingTracingSettings settings = MessagingTracingSettings.enabledByDefault()
                                                                        .withEventSourcingHandlersEnabled(
                                                                                showEventSourcingHandlers);
            configurer.componentRegistry(
                    registry -> registry.registerComponent(MessagingTracingSettings.class, c -> settings));
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
     * Component whose handler method carries the real {@link EventSourcingHandler} annotation — the per-method
     * enhancer must detect it through the annotation pipeline and suppress (or, when enabled, produce) the span
     * {@code BookingState.on(String)}.
     */
    @SuppressWarnings("unused")
    static class BookingState {

        private final AtomicReference<String> handledEvent = new AtomicReference<>();

        @EventSourcingHandler
        public void on(String event) {
            handledEvent.set(event);
        }
    }
}
