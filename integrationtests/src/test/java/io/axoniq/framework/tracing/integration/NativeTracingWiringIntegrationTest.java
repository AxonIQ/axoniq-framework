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
import org.axonframework.messaging.tracing.configuration.SpanAttributesProviderRegistry;
import org.axonframework.messaging.commandhandling.tracing.TracingCommandBus;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the plain-Java (native) tracing wiring: the application registers only a Micrometer-backed
 * {@link SpanFactory} component whose {@link org.axonframework.messaging.tracing.SpanAttributesProvider} list is resolved from
 * the {@link SpanAttributesProviderRegistry} — the documented one-liner — and everything else is contributed by the
 * ServiceLoader-discovered enhancers of the tracing modules: the registry default, the built-in providers driven by
 * the settings defaults, and the tracing decorators.
 * <p>
 * Also pins the off-contract: with no {@link SpanFactory} component registered, the tracing enhancers leave the
 * {@link CommandBus} undecorated (no {@code TracingCommandBus} wrapper), not merely silent.
 */
class NativeTracingWiringIntegrationTest {

    private static final String DISPATCH_SPAN_PREFIX = "CommandBus.dispatchCommand";

    private MicrometerTracingTestSetup tracing;
    private InMemorySpanExporter spanExporter;
    private @Nullable AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        tracing = MicrometerTracingTestSetup.create();
        spanExporter = tracing.spanExporter();
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
        tracing.close();
    }

    private AxonConfiguration startApplication(boolean registerSpanFactory) {
        CommandHandlingModule commandHandlingModule =
                CommandHandlingModule.named("native-tracing-wiring-test")
                                     .commandHandlers()
                                     .autodetectedCommandHandlingComponent(c -> new BookRoomHandler())
                                     .build();
        MessagingConfigurer configurer = MessagingConfigurer.create()
                .componentRegistry(registry -> registry
                        // Stay local: disable the Axon Server connector enhancer.
                        .disableEnhancer(AxonServerConfigurationEnhancer.class))
                .registerCommandHandlingModule(() -> commandHandlingModule);
        if (registerSpanFactory) {
            configurer.componentRegistry(registry -> {
                // The documented plain-Java wiring: one component registration; providers resolve from the registry.
                registry.registerComponent(SpanFactory.class, config -> tracing.spanFactory(
                        config.getComponent(SpanAttributesProviderRegistry.class).providers(config)));
                // A custom application provider, contributed through the canonical idiom.
                SpanAttributesProviderRegistry.register(
                        registry, c -> (message, context) -> Map.of("tenant.id", "acme"));
            });
        }
        return configurer.start();
    }

    @Nested
    class WithSpanFactoryComponent {

        @Test
        void builtInAndCustomProvidersContributeAttributesThroughTheNativeWiring() {
            // given the native wiring with the registry-resolved provider list and a custom provider
            configuration = startApplication(true);
            CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);

            // when a command is dispatched
            String result = commandGateway.send(new BookRoom("room-42"))
                                          .resultAs(String.class)
                                          .orTimeout(30, TimeUnit.SECONDS)
                                          .join();

            // then the dispatch span carries attributes from the built-in providers (contributed by the module
            // enhancers, driven by the settings defaults) and from the custom registry-contributed provider
            assertThat(result).isEqualTo("booked");
            await().atMost(Duration.ofSeconds(30))
                   .untilAsserted(() -> assertThat(spanNames())
                           .anyMatch(name -> name.startsWith(DISPATCH_SPAN_PREFIX)));

            SpanData dispatchSpan = spanStartingWith(DISPATCH_SPAN_PREFIX);
            assertThat(dispatchSpan.getAttributes().get(AttributeKey.stringKey("axoniq.message.id")))
                    .as("built-in axoniq.message.id attribute")
                    .isNotNull();
            assertThat(dispatchSpan.getAttributes().get(AttributeKey.stringKey("axoniq.message.type")))
                    .as("built-in axoniq.message.type attribute")
                    .isNotNull();
            assertThat(dispatchSpan.getAttributes().get(AttributeKey.stringKey("tenant.id")))
                    .as("custom provider attribute")
                    .isEqualTo("acme");
        }

        @Test
        void commandBusIsWrappedWithTheTracingDecorator() {
            // given
            configuration = startApplication(true);

            // when / then — the tracing decorator registers at near-maximal order, so it is the outermost wrapper
            assertThat(configuration.getComponent(CommandBus.class)).isInstanceOf(TracingCommandBus.class);
        }
    }

    @Nested
    class WithoutSpanFactoryComponent {

        @Test
        void commandBusStaysUndecorated() {
            // given no SpanFactory component is registered (tracing off)
            configuration = startApplication(false);

            // when / then — the enhancers leave the bus undecorated entirely; tracing is off with zero overhead
            assertThat(configuration.getComponent(CommandBus.class)).isNotInstanceOf(TracingCommandBus.class);
            assertThat(spanExporter.getFinishedSpanItems()).isEmpty();
        }
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
