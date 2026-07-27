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
import io.axoniq.framework.tracing.micrometer.MicrometerTracingConfigurationEnhancer;
import io.axoniq.framework.tracing.micrometer.threadlocal.MicrometerThreadLocalContextPropagationConfigurationEnhancer;
import org.axonframework.messaging.tracing.SpanFactory;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
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
 * Integration test for command-side tracing through the real configuration: a {@link MessagingConfigurer} registers
 * a Micrometer {@code SpanFactory} as a component, disables the Axon
 * Server connector enhancer so the bus stays local, and registers an annotated {@code @CommandHandler}. Dispatching
 * through the {@link CommandGateway} must produce the full nested span tree {@code CommandBus.dispatch}
 * (PRODUCER) → {@code CommandBus.handle} (CONSUMER) → {@code BookRoomHandler.handle(BookRoom)} (INTERNAL),
 * with no manual decorator or handler-definition wiring (per constitution §Testing). With no {@link SpanFactory}
 * registered, the same wiring produces no spans.
 */
class CommandTracingConfigurationIntegrationTest {

    private static final String ENHANCER_SPAN = "BookRoomHandler.handle(BookRoom)";
    private static final String HANDLE_SPAN_PREFIX = "CommandBus.handle";
    private static final String DISPATCH_SPAN_PREFIX = "CommandBus.dispatch";

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
    void aDispatchedCommandProducesTheFullSpanTreeThroughTheConfigurationDrivenWiring() {
        // given a configured application that registers a Micrometer SpanFactory and an annotated command handler,
        // with the Axon Server connector disabled so the command bus stays local (no Docker required)
        configuration = startApplication(/* registerSpanFactory */ true);
        CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);

        // when a command is dispatched
        String result = commandGateway.send(new BookRoom("room-42"))
                                      .resultAs(String.class)
                                      .orTimeout(30, TimeUnit.SECONDS)
                                      .join();

        // then the dispatch + handle + per-method enhancer spans appear, all sharing one trace, properly nested:
        // dispatch -> handle -> BookRoomHandler.handle(BookRoom)
        assertThat(result).isEqualTo("booked");
        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertThat(spanNames()).contains(ENHANCER_SPAN));

        SpanData dispatchSpan = spanStartingWith(DISPATCH_SPAN_PREFIX);
        SpanData handleSpan = spanStartingWith(HANDLE_SPAN_PREFIX);
        SpanData enhancerSpan = spanNamed(ENHANCER_SPAN);

        assertThat(dispatchSpan.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(handleSpan.getKind()).isEqualTo(SpanKind.CONSUMER);
        assertThat(enhancerSpan.getKind()).isEqualTo(SpanKind.INTERNAL);

        assertThat(handleSpan.getTraceId()).isEqualTo(dispatchSpan.getTraceId());
        assertThat(handleSpan.getParentSpanContext().getSpanId()).isEqualTo(dispatchSpan.getSpanId());

        assertThat(enhancerSpan.getTraceId()).isEqualTo(dispatchSpan.getTraceId());
        assertThat(enhancerSpan.getParentSpanContext().getSpanId()).isEqualTo(handleSpan.getSpanId());
    }

    @Test
    void withoutAConfiguredSpanFactoryNoTracingSpansAreProduced() {
        // given the same configuration but WITHOUT a SpanFactory registered — the tracing decorators gate on
        // SpanFactory presence (and the handler enhancer's lazy resolution gracefully degrades when none is found)
        configuration = startApplication(/* registerSpanFactory */ false);
        CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);

        // when
        String result = commandGateway.send(new BookRoom("room-42"))
                                      .resultAs(String.class)
                                      .orTimeout(30, TimeUnit.SECONDS)
                                      .join();

        // then the command is handled normally, but the InMemorySpanExporter has nothing — tracing is fully off
        assertThat(result).isEqualTo("booked");
        assertThat(spanExporter.getFinishedSpanItems()).isEmpty();
    }

    private AxonConfiguration startApplication(boolean registerSpanFactory) {
        CommandHandlingModule commandHandlingModule =
                CommandHandlingModule.named("tracing-config-test")
                                     .commandHandlers()
                                     .autodetectedCommandHandlingComponent(c -> new BookRoomHandler())
                                     .build();
        MessagingConfigurer configurer = MessagingConfigurer.create()
                                                            .componentRegistry(registry -> registry
                                                                    // Stay local: disable the Axon Server connector
                                                                    // enhancer (it would otherwise register a
                                                                    // CommandBusConnector that flips the bus to
                                                                    // DistributedCommandBus).
                                                                    .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                                            )
                                                            .registerCommandHandlingModule(() -> commandHandlingModule);
        if (registerSpanFactory) {
            configurer.componentRegistry(registry -> registry
                    .registerComponent(SpanFactory.class, c -> spanFactory)
                    .registerComponent(Tracer.class, c -> tracing.tracer()));
        } else {
            // No bridge components available in this scenario -- disable the Micrometer enhancers so they don't
            // fail requiring a Tracer to build their own SpanFactory / thread-local bridge.
            configurer.componentRegistry(registry -> registry
                    .disableEnhancer(MicrometerTracingConfigurationEnhancer.class)
                    .disableEnhancer(MicrometerThreadLocalContextPropagationConfigurationEnhancer.class));
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

    private SpanData spanStartingWith(String prefix) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().startsWith(prefix))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError("No span starting with " + prefix + " in " + spanNames()));
    }

    private record BookRoom(String roomId) {

    }

    @SuppressWarnings("unused")
    static class BookRoomHandler {

        @CommandHandler
        public String handle(BookRoom command) {
            return "booked";
        }
    }
}
