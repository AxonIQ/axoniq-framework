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
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Story-4 acceptance test (SC-003a): the AF5 tracing consolidation must not regress the observable trace shape from
 * AF4. This IT exercises one of each in-scope component (CommandBus, EventBus, QueryBus) through the real
 * {@link MessagingConfigurer} wiring and asserts the span name + kind for every row in af4-span-inventory.md §2 that
 * is reachable on a non-distributed local configuration (no Axon Server / Docker required). The Axon-Server-distributed
 * rows (AxonServer*Bus.dispatch/handle, QueryProcessingTask, ResponseProcessingTask) are covered by the *AxonServerIT
 * companions; deadlines and sagas are explicitly out of scope for AF5.
 */
class AF4SpanShapeParityIntegrationTest {

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
    void commandEventQueryFlowProducesEveryAF4LocalSpan() {
        // given the full local configuration with command + event + query handling
        configuration = startApplication();

        CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);
        EventGateway eventGateway = configuration.getComponent(EventGateway.class);
        QueryGateway queryGateway = configuration.getComponent(QueryGateway.class);

        // when each component is exercised once
        commandGateway.send(new BookRoom("room-42"))
                      .resultAs(String.class)
                      .orTimeout(30, TimeUnit.SECONDS)
                      .join();
        eventGateway.publish(null, List.of("room-42-booked")).orTimeout(30, TimeUnit.SECONDS).join();
        queryGateway.query(new FindRoom("room-42"), String.class, null)
                    .orTimeout(30, TimeUnit.SECONDS)
                    .join();

        // then every retained AF4 counterpart span is produced with the expected name + kind
        // Note: bus-level spans use the message's qualified name (the FQN of the payload type for records without
        // an explicit @MessageType) — we match by prefix to stay robust against nested test-class naming.
        Map<String, SpanKind> prefixExpected = Map.of(
                // §2 row: ${CommandBusClass}.dispatch(${cmd})
                "CommandBus.dispatch", SpanKind.PRODUCER,
                // §2 row: ${CommandBusClass}.handle(${cmd})
                "CommandBus.handle", SpanKind.CONSUMER,
                // §2 row: ${EventBusClass}.publish(${event}) — AF5 names the owning EventSink abstraction
                "EventSink.publish", SpanKind.PRODUCER,
                // §2 row: SimpleQueryBus.query(${q}) — local, non-distributed
                "QueryBus.query ", SpanKind.PRODUCER,
                // AF5 addition: explicit handler span on the consuming side of the query
                "QueryBus.handle", SpanKind.CONSUMER
        );

        prefixExpected.forEach((prefix, kind) -> {
            SpanData span = spanStartingWith(prefix);
            assertThat(span.getKind()).as("Span '" + prefix + "' kind").isEqualTo(kind);
        });
        assertThat(spanExporter.getFinishedSpanItems())
                .extracting(SpanData::getName)
                .noneMatch(name -> name.startsWith("EventBus.commitEvents"));

        // §2 row: ${ContainingClass}.${method}(${args}) — per-handler enhancer spans
        assertThat(spanNamed("BookRoomHandler.handle(BookRoom)").getKind()).isEqualTo(SpanKind.INTERNAL);
        assertThat(spanNamed("FindRoomHandler.handle(FindRoom)").getKind()).isEqualTo(SpanKind.INTERNAL);
    }

    private SpanData spanStartingWith(String prefix) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().startsWith(prefix))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError(
                                   "No span starting with '" + prefix + "' in " + spanExporter.getFinishedSpanItems()
                                           .stream().map(SpanData::getName).toList()));
    }

    private AxonConfiguration startApplication() {
        CommandHandlingModule commands =
                CommandHandlingModule.named("af4-parity-commands")
                                     .commandHandlers()
                                     .autodetectedCommandHandlingComponent(c -> new BookRoomHandler())
                                     .build();
        QueryHandlingModule queries =
                QueryHandlingModule.named("af4-parity-queries")
                                   .queryHandlers()
                                   .autodetectedQueryHandlingComponent(c -> new FindRoomHandler())
                                   .build();
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> registry
                                          .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                          .registerComponent(SpanFactory.class, c -> spanFactory))
                                  .registerCommandHandlingModule(() -> commands)
                                  .registerQueryHandlingModule(() -> queries)
                                  .start();
    }

    private SpanData spanNamed(String name) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().equals(name))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError(
                                   "No span named '" + name + "' in " + spanExporter.getFinishedSpanItems().stream()
                                           .map(SpanData::getName).toList()));
    }

    private record BookRoom(String roomId) {
    }

    private record FindRoom(String roomId) {
    }

    @SuppressWarnings("unused")
    static class BookRoomHandler {

        @CommandHandler
        public String handle(BookRoom command) {
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
