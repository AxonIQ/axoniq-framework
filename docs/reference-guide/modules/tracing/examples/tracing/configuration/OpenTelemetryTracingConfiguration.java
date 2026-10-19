/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package tracing.configuration;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

public final class OpenTelemetryTracingConfiguration {

    // tag::full-open-telemetry[]
    public AxonConfiguration start(String primaryEndpoint, String secondaryEndpoint) {
        SpanExporter spanExporter = compositeExporter(primaryEndpoint, secondaryEndpoint);
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                                                            .addSpanProcessor(
                                                                    BatchSpanProcessor.builder(spanExporter).build()
                                                            )
                                                            .build();
        ContextPropagators contextPropagators =
                ContextPropagators.create(W3CTraceContextPropagator.getInstance());
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                                                         .setTracerProvider(tracerProvider)
                                                         .setPropagators(contextPropagators)
                                                         .build();

        io.opentelemetry.api.trace.Tracer openTelemetryTracer =
                openTelemetry.getTracer("AxoniqFramework");
        Tracer tracer = new OtelTracer(
                openTelemetryTracer,
                new OtelCurrentTraceContext(),
                event -> {
                }
        );
        Propagator propagator = new OtelPropagator(contextPropagators, openTelemetryTracer);

        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> registry
                                          .registerComponent(Tracer.class, configuration -> tracer)
                                          .registerComponent(Propagator.class, configuration -> propagator))
                                  .lifecycleRegistry(registry -> registry.onShutdown(tracerProvider::close))
                                  .start();
    }

    // tag::multiple-exporters[]
    private SpanExporter compositeExporter(String primaryEndpoint, String secondaryEndpoint) {
        SpanExporter primary = OtlpGrpcSpanExporter.builder()
                                                   .setEndpoint(primaryEndpoint)
                                                   .build();
        SpanExporter secondary = OtlpGrpcSpanExporter.builder()
                                                     .setEndpoint(secondaryEndpoint)
                                                     .build();
        return SpanExporter.composite(primary, secondary);
    }
    // end::multiple-exporters[]
    // end::full-open-telemetry[]
}
