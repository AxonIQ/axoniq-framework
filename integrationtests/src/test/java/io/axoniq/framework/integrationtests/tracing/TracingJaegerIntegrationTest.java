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

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.MultiTenancyUtils;
import io.axoniq.framework.tracing.micrometer.MicrometerSpanFactory;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.tracing.SpanFactory;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Realistic-backend tracing integration test: exports spans to a Jaeger all-in-one container via OTLP
 * and asserts them via Jaeger's HTTP API. Auto-skips when Docker is unavailable (Testcontainers' built-in detection
 * via {@link GenericContainer}). Runs under the {@code integration-test} Maven profile alongside the other
 * {@code *IT} suites.
 * <p>
 * The trace shape asserted here is the simplest in-scope command flow ({@code CommandBus.dispatch} ->
 * {@code CommandBus.handle} -> {@code @CommandHandler} enhancer span); the full multi-component trace tree is
 * exercised by {@link MicrometerTracingEndToEndIntegrationTest} against the {@code InMemorySpanExporter}.
 */
@Testcontainers
class TracingJaegerIntegrationTest {

    private static final int JAEGER_OTLP_HTTP_PORT = 4318;
    private static final int JAEGER_QUERY_PORT = 16686;
    private static final String SERVICE_NAME = "tracing-jaeger-it";

    private GenericContainer<?> jaeger;
    private SdkTracerProvider tracerProvider;
    private SpanFactory spanFactory;
    private @Nullable AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        // jaegertracing/all-in-one bundles an in-memory storage backend, the OTLP receiver and the query HTTP API.
        jaeger = new GenericContainer<>(DockerImageName.parse("jaegertracing/all-in-one:1.62.0"))
                .withExposedPorts(JAEGER_OTLP_HTTP_PORT, JAEGER_QUERY_PORT)
                .withEnv("COLLECTOR_OTLP_ENABLED", "true")
                .waitingFor(Wait.forHttp("/").forPort(JAEGER_QUERY_PORT).withStartupTimeout(Duration.ofMinutes(2)));
        jaeger.start();

        OtlpHttpSpanExporter otlpExporter = OtlpHttpSpanExporter.builder()
                                                                .setEndpoint("http://" + jaeger.getHost() + ":"
                                                                                     + jaeger.getMappedPort(JAEGER_OTLP_HTTP_PORT)
                                                                                     + "/v1/traces")
                                                                .build();
        tracerProvider = SdkTracerProvider.builder()
                                          .addSpanProcessor(BatchSpanProcessor.builder(otlpExporter)
                                                                              .setScheduleDelay(100, TimeUnit.MILLISECONDS)
                                                                              .build())
                                          .setResource(Resource.builder()
                                                               .put("service.name", SERVICE_NAME)
                                                               .build())
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
        spanFactory = new MicrometerSpanFactory(tracer, propagator);
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
        if (tracerProvider != null) {
            tracerProvider.close();
        }
        if (jaeger != null) {
            jaeger.stop();
        }
    }

    @Test
    void aTracedCommandFlowEndsUpInJaeger() {
        // given the standard MessagingConfigurer + OTLP exporter pointed at the Jaeger container
        CommandHandlingModule commands =
                CommandHandlingModule.named("jaeger-test")
                                     .commandHandlers()
                                     .autodetectedCommandHandlingComponent(c -> new GreetHandler())
                                     .build();
        configuration = MessagingConfigurer.create()
                                           // Not a multi-tenancy test. See MultiTenancyUtils#disable.
                                           .componentRegistry(MultiTenancyUtils::disable)
                                           .componentRegistry(registry -> registry
                                                   .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                                   .registerComponent(SpanFactory.class, c -> spanFactory))
                                           .registerCommandHandlingModule(() -> commands)
                                           .start();
        CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);

        // when a command is dispatched (the tracing decorators emit dispatch + handle + enhancer spans, the OTLP
        // exporter batches them to Jaeger)
        String result = commandGateway.send(new Greet("world"))
                                      .resultAs(String.class)
                                      .orTimeout(30, TimeUnit.SECONDS)
                                      .join();
        assertThat(result).isEqualTo("hello world");

        // then Jaeger's HTTP query API reports a trace with the expected operation names
        String jaegerHost = jaeger.getHost();
        int jaegerPort = jaeger.getMappedPort(JAEGER_QUERY_PORT);
        HttpClient http = HttpClient.newHttpClient();

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1))
               .untilAsserted(() -> {
                   String body = queryJaeger(http, jaegerHost, jaegerPort);
                   assertThat(body).contains("CommandBus.dispatch")
                                   .contains("CommandBus.handle")
                                   .contains("GreetHandler.handle");
               });
    }

    private static String queryJaeger(HttpClient http, String host, int port) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest
                .newBuilder(URI.create("http://" + host + ":" + port + "/api/traces?service=" + SERVICE_NAME))
                .GET()
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private record Greet(String who) {
    }

    @SuppressWarnings("unused")
    static class GreetHandler {

        @CommandHandler
        public String handle(Greet command) {
            return "hello " + command.who();
        }
    }
}
