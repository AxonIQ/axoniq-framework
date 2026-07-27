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
import io.axoniq.framework.tracing.micrometer.MicrometerSpanFactory;
import io.axoniq.framework.tracing.micrometer.MicrometerTracingConfigurationEnhancer;
import io.micrometer.context.ContextSnapshotFactory;
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
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tracing integration test for the <b>Micrometer</b> binding, wired through the real
 * {@link MessagingConfigurer} (Axon Server connector disabled to keep it local). Only the backend components — a
 * {@link Tracer} and {@link Propagator} over Micrometer Tracing's OpenTelemetry bridge — are registered; the
 * ServiceLoader-discovered {@link MicrometerTracingConfigurationEnhancer} then builds the {@link MicrometerSpanFactory}
 * and installs the thread-local bridge. Spans are captured with an in-memory OTel exporter. It proves two things:
 * <ol>
 *     <li><b>Span-tree parity</b>: a command and a query each produce the same nested span tree the OpenTelemetry
 *     binding produces ({@code *.dispatch/query} → {@code *.handle} → handler-method span), one trace each.</li>
 *     <li><b>Deep context propagation</b>: the thread-local context-propagation bridge (auto-installed as a
 *     {@code UnitOfWorkFactory} decorator via ServiceLoader) makes the Axon span thread-local-current <em>inside the
 *     handler</em>, so {@code tracer.currentSpan()} observed from the handler body is non-null and on the command's
 *     trace — this is what lets instrumented JDBC/gRPC/WebClient calls and MDC logging nest correctly.</li>
 * </ol>
 */
class MicrometerTracingEndToEndIntegrationTest {

    private static final AtomicReference<@Nullable String> CURRENT_TRACE_ID_IN_HANDLER = new AtomicReference<>();
    private static @Nullable Tracer handlerTracer;

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private Tracer tracer;
    private Propagator propagator;
    private @Nullable AxonConfiguration configuration;

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
        propagator = new OtelPropagator(contextPropagators, otelTracer);
        handlerTracer = tracer;
        CURRENT_TRACE_ID_IN_HANDLER.set(null);
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
        tracerProvider.close();
        handlerTracer = null;
    }

    @Test
    void aCommandAndAQueryEachProduceTheExpectedNestedSpanTreeAndDoNotShareATrace() {
        // given an end-to-end configuration with both command and query handling wired up
        configuration = startApplication();
        CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);
        QueryGateway queryGateway = configuration.getComponent(QueryGateway.class);

        // when a command is dispatched and then a query is dispatched
        String commandResult = commandGateway.send(new BookRoom("room-42"))
                                             .resultAs(String.class)
                                             .orTimeout(30, TimeUnit.SECONDS)
                                             .join();
        String queryResult = queryGateway.query(new FindRoom("room-42"), String.class, null)
                                         .orTimeout(30, TimeUnit.SECONDS)
                                         .join();

        // then both succeed and the expected span trees show up
        assertThat(commandResult).isEqualTo("booked");
        assertThat(queryResult).isEqualTo("room-42-info");

        SpanData commandDispatch = spanStartingWith("CommandBus.dispatch");
        SpanData commandHandle = spanStartingWith("CommandBus.handle");
        SpanData commandHandler = spanNamed("BookRoomHandler.handle(BookRoom)");

        SpanData queryDispatch = spanStartingWith("QueryBus.query");
        SpanData queryHandle = spanStartingWith("QueryBus.handle");
        SpanData queryHandler = spanNamed("FindRoomHandler.handle(FindRoom)");

        // Command trace is internally connected.
        assertThat(commandHandle.getTraceId()).isEqualTo(commandDispatch.getTraceId());
        assertThat(commandHandler.getTraceId()).isEqualTo(commandDispatch.getTraceId());
        assertThat(commandHandle.getParentSpanContext().getSpanId()).isEqualTo(commandDispatch.getSpanId());
        assertThat(commandHandler.getParentSpanContext().getSpanId()).isEqualTo(commandHandle.getSpanId());

        // Query trace is internally connected.
        assertThat(queryHandle.getTraceId()).isEqualTo(queryDispatch.getTraceId());
        assertThat(queryHandler.getTraceId()).isEqualTo(queryDispatch.getTraceId());
        assertThat(queryHandle.getParentSpanContext().getSpanId()).isEqualTo(queryDispatch.getSpanId());
        assertThat(queryHandler.getParentSpanContext().getSpanId()).isEqualTo(queryHandle.getSpanId());

        // Independent dispatches produce independent traces.
        assertThat(commandDispatch.getTraceId()).isNotEqualTo(queryDispatch.getTraceId());
    }

    @Test
    void theAxonSpanIsThreadLocalCurrentInsideTheHandlerViaTheContextPropagationBridge() {
        // given
        configuration = startApplication();
        CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);

        // when a command is handled (the handler records tracer.currentSpan() observed from its own body)
        commandGateway.send(new BookRoom("room-7"))
                      .resultAs(String.class)
                      .orTimeout(30, TimeUnit.SECONDS)
                      .join();

        // then a span was current inside the handler and it belongs to the command's trace — the bridge propagated it
        SpanData commandDispatch = spanStartingWith("CommandBus.dispatch");
        assertThat(CURRENT_TRACE_ID_IN_HANDLER.get())
                .as("tracer.currentSpan() inside the handler")
                .isNotNull()
                .isEqualTo(commandDispatch.getTraceId());
    }

    @Test
    void threadLocalContextPropagationBridgeIsInstalledByDefault() {
        // given / when
        configuration = startApplication();

        // then the ServiceLoader thread-local enhancer ran (registering its ContextSnapshotFactory) and decorated the
        // UnitOfWorkFactory
        assertThat(configuration.getOptionalComponent(ContextSnapshotFactory.class)).isPresent();
    }

    private AxonConfiguration startApplication() {
        CommandHandlingModule commandModule =
                CommandHandlingModule.named("micrometer-e2e-tracing-commands")
                                     .commandHandlers()
                                     .autodetectedCommandHandlingComponent(c -> new BookRoomHandler())
                                     .build();
        QueryHandlingModule queryModule =
                QueryHandlingModule.named("micrometer-e2e-tracing-queries")
                                   .queryHandlers()
                                   .autodetectedQueryHandlingComponent(c -> new FindRoomHandler())
                                   .build();
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> registry
                                          .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                          .registerComponent(Tracer.class, c -> tracer)
                                          .registerComponent(Propagator.class, c -> propagator))
                                  .registerCommandHandlingModule(() -> commandModule)
                                  .registerQueryHandlingModule(() -> queryModule)
                                  .start();
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

    private SpanData spanStartingWith(String prefix) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().startsWith(prefix))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError(
                                   "No span starting with " + prefix + " in " + spanNames()));
    }

    private record BookRoom(String roomId) {
    }

    private record FindRoom(String roomId) {
    }

    @SuppressWarnings("unused")
    static class BookRoomHandler {

        @CommandHandler
        public String handle(BookRoom command) {
            Tracer tracer = handlerTracer;
            if (tracer != null && tracer.currentSpan() != null) {
                CURRENT_TRACE_ID_IN_HANDLER.set(tracer.currentSpan().context().traceId());
            }
            return "booked";
        }
    }

    @SuppressWarnings("unused")
    static class FindRoomHandler {

        @QueryHandler
        public String handle(FindRoom query) {
            return query.roomId() + "-info";
        }
    }
}
