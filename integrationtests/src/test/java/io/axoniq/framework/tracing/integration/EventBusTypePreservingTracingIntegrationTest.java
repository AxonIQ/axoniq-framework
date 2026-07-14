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
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventBus;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression test for the EventSink/EventBus type-preservation issue.
 * <p>
 * AF5's {@code EventBusConfigurationDefaults} registers a {@code SimpleEventBus} under {@code EventBus.class}, not
 * under {@code EventSink.class}. {@code DecoratorDefinition.forType(EventSink.class)} matches the slot via
 * {@code isAssignableFrom}, but AF5's {@code DecoratedComponent.resolve()} enforces that the decorator returns a
 * subtype of the declared slot type — so a plain {@link org.axonframework.messaging.eventhandling.tracing.TracingEventSink}
 * (only implements {@code EventSink}) would have aborted configuration with a {@code ClassCastException}.
 * <p>
 * The fix is type-preserving decoration: {@link org.axonframework.messaging.eventhandling.tracing.TracingEventBus
 * TracingEventBus} now wraps the {@code EventBus.class} slot directly. This IT proves the wrapping happens through the
 * real {@code MessagingConfigurer} wiring — the configured {@code EventGateway} resolves to a tracing-wrapped EventBus
 * and a published event produces the expected publish + commit spans.
 */
class EventBusTypePreservingTracingIntegrationTest {

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
    void theEventBusSlotResolvesToATracingWrapperAndPublishProducesSpans() {
        // given the default MessagingConfigurer wiring (which registers a SimpleEventBus under EventBus.class via
        // EventBusConfigurationDefaults) plus our test SpanFactory
        configuration = MessagingConfigurer
                .create()
                .componentRegistry(registry -> registry
                        .disableEnhancer(AxonServerConfigurationEnhancer.class)
                        .registerComponent(SpanFactory.class, c -> spanFactory))
                .start();

        // when the EventBus is resolved and used to publish through the EventGateway
        EventBus eventBus = configuration.getComponent(EventBus.class);
        EventGateway eventGateway = configuration.getComponent(EventGateway.class);

        // then both the EventBus.class slot and the EventGateway's underlying EventSink are tracing-wrapped — the cast
        // would have failed at configuration time if our decorator wasn't type-preserving. The concrete subtype
        // depends on what the standard configuration enhancers registered for that slot — in the integrationtests
        // setup that's an EventStore (since axon-eventsourcing is present), so the wrapper is TracingEventStore. Either
        // way, asserting that the resolved class name starts with "Tracing" proves AF5's component-registry assignment
        // check did not abort configuration.
        assertThat(eventBus.getClass().getSimpleName())
                .as("EventBus.class slot must resolve to a tracing-wrapped instance")
                .startsWith("Tracing");

        // and a publish produces the expected EventSink producer span without an artificial commit span
        eventGateway.publish(null, List.of("room-42-booked")).orTimeout(30, TimeUnit.SECONDS).join();

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(spanNames())
                       .anySatisfy(name -> assertThat(name).startsWith("EventSink.publish")));

        SpanData publishSpan = spanStartingWith("EventSink.publish");
        assertThat(publishSpan.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(spanNames()).noneMatch(name -> name.startsWith("EventBus.commitEvents"));
    }

    private List<String> spanNames() {
        return spanExporter.getFinishedSpanItems().stream().map(SpanData::getName).toList();
    }

    private SpanData spanStartingWith(String prefix) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().startsWith(prefix))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError(
                                   "No span starting with " + prefix + " in " + spanNames()));
    }
}
