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

package io.axoniq.framework.tracing.micrometer;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.tracing.Span;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the "Zone A" contract of a dispatch performed with a {@code null} {@link
 * org.axonframework.messaging.core.unitofwork.ProcessingContext}: message metadata is the durable correlation carrier
 * (a {@code traceparent} is injected before any dispatch interceptor runs), and the dispatch span is the current span
 * while the synchronous dispatch segment executes (via the {@code run*} wrap), so metadata-based dispatch interceptors
 * correlate correctly. Also proves that a metadata-stripping interceptor degrades the downstream handler span
 * gracefully to a fresh root instead of failing.
 */
class DispatchInterceptorVisibilityTest {

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private Tracer tracer;
    private MicrometerSpanFactory factory;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                                          .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                                          .build();
        ContextPropagators contextPropagators = ContextPropagators.create(W3CTraceContextPropagator.getInstance());
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                                                         .setTracerProvider(tracerProvider)
                                                         .setPropagators(contextPropagators)
                                                         .build();
        io.opentelemetry.api.trace.Tracer otelTracer = openTelemetry.getTracer("AxoniqFramework");
        tracer = new OtelTracer(otelTracer, new OtelCurrentTraceContext(), event -> {
        });
        Propagator propagator = new OtelPropagator(contextPropagators, otelTracer);
        factory = new MicrometerSpanFactory(tracer, propagator);
    }

    @AfterEach
    void tearDown() {
        tracerProvider.close();
    }

    private static Message anEvent() {
        return EventTestUtils.asEventMessage("MyPayload");
    }

    @Test
    void dispatchSpanIsCurrentAndMetadataCarriesTraceparentDuringTheSynchronousSegment() {
        // given a dispatch span created with a null ProcessingContext (the caller-thread dispatch, Zone A)
        Span dispatchSpan = factory.createDispatchSpan("dispatch", anEvent(), null);
        AtomicReference<String> currentSpanIdInside = new AtomicReference<>();
        AtomicReference<Message> propagatedInside = new AtomicReference<>();

        // when the synchronous dispatch segment runs inside the span's branch (as the tracing bus decorator does),
        // a dispatch interceptor observes the ambient current span and the already-propagated metadata
        dispatchSpan.branch(null, ignored -> {
            currentSpanIdInside.set(tracer.currentSpan().context().spanId());
            propagatedInside.set(dispatchSpan.propagateContext(anEvent()));
            return null;
        });

        // then (b) the dispatch span was current, and (a) metadata carries the propagated traceparent
        SpanData dispatched = spanExporter.getFinishedSpanItems().stream()
                                          .filter(s -> s.getName().equals("dispatch"))
                                          .findFirst().orElseThrow();
        assertThat(currentSpanIdInside.get()).isEqualTo(dispatched.getSpanId());
        assertThat(propagatedInside.get().metadata()).containsKey("traceparent");
    }

    @Test
    void handlerDegradesToRootWhenAnInterceptorStripsTheTraceMetadata() {
        // given a message that carried a propagated dispatch context
        Span dispatchSpan = factory.createDispatchSpan("dispatch", anEvent(), null);
        Message propagated;
        try (var ignored = dispatchSpan.start()) {
            propagated = dispatchSpan.propagateContext(anEvent());
        }
        assertThat(propagated.metadata()).containsKey("traceparent");

        // when a dispatch interceptor strips the reserved propagation keys before the handler span is created
        Message stripped = anEvent();
        Span handlerSpan = factory.createHandlerSpan("handler", stripped, null);
        handlerSpan.start().close();

        // then the handler starts a fresh root instead of failing or joining a stale trace
        SpanData handler = spanExporter.getFinishedSpanItems().stream()
                                       .filter(s -> s.getName().equals("handler"))
                                       .findFirst().orElseThrow();
        assertThat(handler.getParentSpanContext().isValid()).isFalse();
    }
}
