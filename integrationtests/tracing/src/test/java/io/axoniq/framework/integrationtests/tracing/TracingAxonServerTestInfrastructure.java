/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.tracing;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.attributes.MessageIdSpanAttributesProvider;
import org.axonframework.messaging.tracing.attributes.MessageTypeSpanAttributesProvider;

import java.util.List;

/**
 * {@link TestInfrastructure} that delegates container lifecycle to {@link AxonServerTestInfrastructure} while also
 * registering a Micrometer-backed {@link SpanFactory} component so the ServiceLoader-discovered
 * {@code MessagingTracingConfigurationEnhancer} wraps the (distributed) {@link org.axonframework.messaging.commandhandling.CommandBus}
 * with the tracing decorator.
 * <p>
 * The tracing stack is assembled by {@link MicrometerTracingTestSetup}: an in-JVM OpenTelemetry SDK behind Micrometer
 * Tracing that records spans into an {@link InMemorySpanExporter} and propagates W3C trace context, which is what
 * carries the {@code traceparent} on the command metadata across the Axon Server gRPC boundary. The captured spans are
 * exposed via {@link #spanExporter()} for assertions.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
final class TracingAxonServerTestInfrastructure implements TestInfrastructure {

    private final AxonServerTestInfrastructure delegate = new AxonServerTestInfrastructure();
    private final MicrometerTracingTestSetup tracing = MicrometerTracingTestSetup.create();
    private final SpanFactory spanFactory;

    TracingAxonServerTestInfrastructure() {
        this.spanFactory = tracing.spanFactory(
                List.of(new MessageIdSpanAttributesProvider(), new MessageTypeSpanAttributesProvider()));
    }

    InMemorySpanExporter spanExporter() {
        return tracing.spanExporter();
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
        tracing.close();
    }
}
