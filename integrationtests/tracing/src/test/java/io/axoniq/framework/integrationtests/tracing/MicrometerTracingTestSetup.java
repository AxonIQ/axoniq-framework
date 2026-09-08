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

package io.axoniq.framework.integrationtests.tracing;

import io.axoniq.framework.tracing.micrometer.MicrometerSpanFactory;
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
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.axonframework.messaging.tracing.SpanAttributesProvider;

import java.util.List;

/**
 * Test fixture assembling the shipping {@link MicrometerSpanFactory} over Micrometer Tracing's OpenTelemetry bridge,
 * backed by an in-memory OpenTelemetry SDK. This is the standard harness for the tracing integration tests: it exposes
 * an {@link InMemorySpanExporter} to assert on exported spans and a factory method producing the same
 * {@link MicrometerSpanFactory} the framework wires at runtime, so the tests exercise the binding that actually ships.
 * <p>
 * {@link #close()} shuts down the underlying {@link SdkTracerProvider}; call it from the test's {@code @AfterEach}.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
final class MicrometerTracingTestSetup implements AutoCloseable {

    private final InMemorySpanExporter spanExporter;
    private final SdkTracerProvider tracerProvider;
    private final Tracer tracer;
    private final Propagator propagator;

    private MicrometerTracingTestSetup(InMemorySpanExporter spanExporter,
                                       SdkTracerProvider tracerProvider,
                                       Tracer tracer,
                                       Propagator propagator) {
        this.spanExporter = spanExporter;
        this.tracerProvider = tracerProvider;
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /**
     * Assembles a fresh in-memory Micrometer tracing harness.
     *
     * @return a new setup backed by its own {@link InMemorySpanExporter}
     */
    static MicrometerTracingTestSetup create() {
        InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                                                            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                                                            .build();
        ContextPropagators contextPropagators = ContextPropagators.create(W3CTraceContextPropagator.getInstance());
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                                                         .setTracerProvider(tracerProvider)
                                                         .setPropagators(contextPropagators)
                                                         .build();
        io.opentelemetry.api.trace.Tracer otelTracer = openTelemetry.getTracer("AxoniqFramework");
        Tracer tracer = new OtelTracer(otelTracer, new OtelCurrentTraceContext(), event -> {
        });
        Propagator propagator = new OtelPropagator(contextPropagators, otelTracer);
        return new MicrometerTracingTestSetup(spanExporter, tracerProvider, tracer, propagator);
    }

    /**
     * @return the in-memory exporter capturing every finished span produced through this setup
     */
    InMemorySpanExporter spanExporter() {
        return spanExporter;
    }

    /**
     * @return the Micrometer tracer backing this setup (e.g. for {@code tracer.currentSpan()} assertions)
     */
    Tracer tracer() {
        return tracer;
    }

    /**
     * @return the Micrometer propagator backing this setup
     */
    Propagator propagator() {
        return propagator;
    }

    /**
     * @return a {@link MicrometerSpanFactory} with no {@link SpanAttributesProvider SpanAttributesProviders}
     */
    MicrometerSpanFactory spanFactory() {
        return new MicrometerSpanFactory(tracer, propagator);
    }

    /**
     * @param attributesProviders the providers contributing attributes to every message-carrying span
     * @return a {@link MicrometerSpanFactory} with the given {@code attributesProviders}
     */
    MicrometerSpanFactory spanFactory(List<SpanAttributesProvider> attributesProviders) {
        return new MicrometerSpanFactory(tracer, propagator, attributesProviders);
    }

    @Override
    public void close() {
        tracerProvider.close();
    }
}
