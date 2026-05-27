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

import io.axoniq.framework.tracing.SpanNames;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test proving that distributed-tracing context propagates across a real Axon Server (gRPC) boundary.
 * <p>
 * A single application connects to Axon Server and dispatches a command <em>to itself</em> through the server. The
 * dispatch and handle both happen in the same JVM, so one OpenTelemetry SDK / exporter captures both spans. The point
 * is the gRPC round-trip through Axon Server: it verifies the W3C {@code traceparent} rides on the command's metadata
 * across the server boundary (the no-ThreadLocal propagation contract).
 * <p>
 * The expected outcome: a {@code CommandBus.dispatchCommand <name>} span (kind {@link SpanKind#PRODUCER}) and a
 * {@code CommandBus.handleCommand <name>} span (kind {@link SpanKind#CONSUMER}) that share the same trace, with the
 * handle span parented on the dispatch span.
 */
class CommandBusTracingAxonServerIT {

    private static final TracingAxonServerTestInfrastructure INFRASTRUCTURE =
            new TracingAxonServerTestInfrastructure();

    private static final QualifiedName COMMAND_NAME = new QualifiedName(GreetCommand.class);

    private AxonConfiguration startedConfiguration;

    @AfterEach
    void tearDown() {
        if (startedConfiguration != null) {
            try {
                startedConfiguration.shutdown();
            } finally {
                INFRASTRUCTURE.stop();
            }
        }
    }

    @Test
    void traceparentPropagatesAcrossAxonServerSoDispatchAndHandleSpansShareATrace() {
        // given
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.spanExporter().reset();

        CommandHandlingModule commandHandlingModule =
                CommandHandlingModule.named("tracing-slice-module")
                                     .commandHandlers()
                                     .commandHandler(
                                             COMMAND_NAME,
                                             (command, context) -> MessageStream.just(new GenericCommandResultMessage(
                                                     new MessageType("greeting"), "handled"
                                             ))
                                     )
                                     .build();

        ApplicationConfigurer configurer =
                MessagingConfigurer.create()
                                   .registerCommandHandlingModule(() -> commandHandlingModule)
                                   .componentRegistry(INFRASTRUCTURE::configureInfrastructure);
        startedConfiguration = configurer.start();
        CommandGateway commandGateway = startedConfiguration.getComponent(CommandGateway.class);

        // when
        String result = commandGateway.send(new GreetCommand("world"))
                                      .resultAs(String.class)
                                      .orTimeout(30, java.util.concurrent.TimeUnit.SECONDS)
                                      .join();

        // then
        assertThat(result).isEqualTo("handled");

        InMemorySpanExporter exporter = INFRASTRUCTURE.spanExporter();
        String commandName = COMMAND_NAME.name();
        String dispatchSpanName = SpanNames.COMMAND_DISPATCH + " " + commandName;
        String handleSpanName = SpanNames.COMMAND_HANDLE + " " + commandName;

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> {
                   List<SpanData> spans = exporter.getFinishedSpanItems();
                   assertThat(spans).extracting(SpanData::getName)
                                    .contains(dispatchSpanName, handleSpanName);
               });

        List<SpanData> spans = exporter.getFinishedSpanItems();
        SpanData dispatchSpan = spanByName(spans, dispatchSpanName);
        SpanData handleSpan = spanByName(spans, handleSpanName);

        assertThat(dispatchSpan.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(handleSpan.getKind()).isEqualTo(SpanKind.CONSUMER);

        // The key assertion: the traceparent rode across the Axon Server gRPC boundary on the command metadata,
        // so the handle span is part of the SAME trace as the dispatch span and is parented on it.
        assertThat(handleSpan.getTraceId())
                .as("handle span trace id must equal dispatch span trace id (same trace across Axon Server)")
                .isEqualTo(dispatchSpan.getTraceId());
        assertThat(handleSpan.getParentSpanId())
                .as("handle span must be parented on the dispatch span")
                .isEqualTo(dispatchSpan.getSpanId());
    }

    private static SpanData spanByName(List<SpanData> spans, String name) {
        Optional<SpanData> match = spans.stream()
                                        .filter(span -> span.getName().equals(name))
                                        .findFirst();
        assertThat(match).as("expected a span named '%s'", name).isPresent();
        return match.get();
    }

    /**
     * Command dispatched through the Axon Server connector to a handler registered in the same application.
     *
     * @param who the greeting target
     */
    record GreetCommand(String who) {

    }
}
