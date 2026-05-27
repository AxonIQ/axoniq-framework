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

package io.axoniq.framework.tracing.slice;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.tracing.SpanFactory;
import io.axoniq.framework.tracing.attributes.MessageIdSpanAttributesProvider;
import io.axoniq.framework.tracing.attributes.MessageNameSpanAttributesProvider;
import io.axoniq.framework.tracing.attributes.MessageTypeSpanAttributesProvider;
import io.axoniq.framework.tracing.attributes.PayloadTypeSpanAttributesProvider;
import io.axoniq.framework.tracing.opentelemetry.OpenTelemetrySpanFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;

/**
 * {@link TestInfrastructure} that delegates container lifecycle to {@link AxonServerTestInfrastructure} while also
 * registering an OpenTelemetry-backed {@link SpanFactory} component so the ServiceLoader-discovered
 * {@code MessagingTracingConfigurationEnhancer} wraps the (distributed) {@link org.axonframework.messaging.commandhandling.CommandBus}
 * with the tracing decorator.
 * <p>
 * The {@link OpenTelemetry} instance is an in-JVM SDK that records spans into an {@link InMemorySpanExporter} and is
 * configured with the {@link W3CTraceContextPropagator}, which is what carries the {@code traceparent} on the command
 * metadata across the Axon Server gRPC boundary. The captured spans are exposed via {@link #spanExporter()} for
 * assertions.
 *
 * @since 5.2.0
 */
final class TracingAxonServerTestInfrastructure implements TestInfrastructure {

    private final AxonServerTestInfrastructure delegate = new AxonServerTestInfrastructure();
    private final InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
    private final OpenTelemetrySpanFactory spanFactory;

    TracingAxonServerTestInfrastructure() {
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                                                            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                                                            .build();
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                                                      .setTracerProvider(tracerProvider)
                                                      .setPropagators(ContextPropagators.create(
                                                              W3CTraceContextPropagator.getInstance()))
                                                      .build();
        this.spanFactory = new OpenTelemetrySpanFactory(openTelemetry);
        this.spanFactory.registerAttributesProvider(new MessageNameSpanAttributesProvider());
        this.spanFactory.registerAttributesProvider(new MessageIdSpanAttributesProvider());
        this.spanFactory.registerAttributesProvider(new MessageTypeSpanAttributesProvider());
        this.spanFactory.registerAttributesProvider(new PayloadTypeSpanAttributesProvider());
    }

    InMemorySpanExporter spanExporter() {
        return spanExporter;
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void configureInfrastructure(ComponentRegistry registry) {
        delegate.configureInfrastructure(registry);
        registry.registerComponent(SpanFactory.class, c -> spanFactory);
    }

    @Override
    public void purgeData() {
        delegate.purgeData();
    }

    @Override
    public void stop() {
        delegate.stop();
    }
}
