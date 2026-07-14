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

import org.axonframework.messaging.tracing.Span;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.SpanScope;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies SC-009: cross-process W3C trace-context propagation through a {@link org.axonframework.messaging.core.Message}'s
 * {@link Metadata}. Rather than requiring a real distributed transport (Axon Server / gRPC), this IT simulates the
 * boundary in-process by rebuilding a fresh {@link EventMessage} with only the {@code traceparent}/{@code tracestate}
 * entries from the publisher's metadata — i.e. the exact subset a remote bus reconstructs on the consuming side after
 * decoding the gRPC headers.
 * <p>
 * The publisher and the consumer use the <em>same</em> OpenTelemetry SDK so the in-memory exporter can compare span
 * IDs — but the consumer has no in-process active span (the simulated transport hop discards the
 * {@link io.opentelemetry.context.Context} value), so the only way the consumer's handler span can share the
 * publisher's trace is if it reads the trace context from the propagated metadata. The assertion confirms that.
 */
class TracingPropagationIntegrationTest {

    private MicrometerTracingTestSetup tracing;
    private InMemorySpanExporter spanExporter;
    private SpanFactory spanFactory;

    @BeforeEach
    void setUp() {
        tracing = MicrometerTracingTestSetup.create();
        spanExporter = tracing.spanExporter();
        spanFactory = tracing.spanFactory();
    }

    @AfterEach
    void tearDown() {
        tracing.close();
    }

    @Test
    void w3CTraceContextSurvivesAMetadataSerializeDeserializeRoundtrip() {
        // given a publisher trace: open a dispatch span, propagate it onto an outgoing event
        EventMessage published = new GenericEventMessage(new MessageType("MyEvent"), "payload");
        Span publishSpan = spanFactory.createDispatchSpan("EventSink.publish MyEvent", published, null);
        SpanScope publishScope = publishSpan.start();
        EventMessage propagated = publishSpan.propagateContext(published);
        publishScope.close();

        // when the transport boundary is simulated: only the traceparent / tracestate metadata entries survive the hop
        EventMessage onTheWire = stripToTraceContextOnly(propagated);

        // and the consuming side opens a handler span using only what's on the message metadata (no active context)
        Span handlerSpan = spanFactory.createHandlerSpan("EventProcessor.process MyEvent", onTheWire,
                                                         new StubProcessingContext());
        handlerSpan.start().close();

        // then both spans live in the same trace (the trace context survived the simulated wire hop)
        SpanData publish = spanNamed("EventSink.publish MyEvent");
        SpanData handler = spanNamed("EventProcessor.process MyEvent");
        assertThat(handler.getTraceId())
                .as("Handler trace id must match publisher trace id (propagation through Metadata)")
                .isEqualTo(publish.getTraceId());
        assertThat(handler.getParentSpanContext().getSpanId())
                .as("Handler must parent on publisher span")
                .isEqualTo(publish.getSpanId());
    }

    /**
     * Discards every metadata entry except the W3C trace-context keys. This is the in-process equivalent of the
     * decoded gRPC request hitting the consuming bus: the original Message object isn't shared, only the wire
     * representation of the metadata is.
     */
    private static EventMessage stripToTraceContextOnly(EventMessage source) {
        Map<String, String> tracingOnly = new HashMap<>();
        source.metadata().forEach((key, value) -> {
            if (isTracingHeader(key)) {
                tracingOnly.put(key, value);
            }
        });
        return new GenericEventMessage(source.type(), source.payload()).withMetadata(Metadata.from(tracingOnly));
    }

    private static boolean isTracingHeader(String key) {
        String lower = key.toLowerCase();
        return lower.equals("traceparent") || lower.equals("tracestate");
    }

    private SpanData spanNamed(String name) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().equals(name))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError("No span named '" + name + "'"));
    }
}
