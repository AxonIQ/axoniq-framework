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

import io.axoniq.framework.messaging.commandhandling.distributed.tracing.TracingCommandBusConnector;
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
 * dispatch and handle both happen in the same JVM, so one OpenTelemetry SDK / exporter captures every span. The point
 * is the gRPC round-trip through Axon Server: it verifies the W3C {@code traceparent} rides on the command's metadata
 * across the server boundary (the no-ThreadLocal propagation contract).
 * <p>
 * Axon Server <em>is</em> the distributed transport, so {@code axoniq-distributed-messaging} is always on the test
 * classpath. The expected span tree is therefore the full four-span chain:
 * <pre>
 * CommandBus.dispatch <name>     [PRODUCER]   ← TracingCommandBus dispatch
 *   └─ CommandBusConnector.dispatch <name>     [PRODUCER]   ← TracingCommandBusConnector send leg (gRPC out)
 *      └─ CommandBusConnector.handle <name>    [CONSUMER]   ← TracingCommandBusConnector receive leg (gRPC in)
 *         └─ CommandBus.handle <name>   [CONSUMER]   ← TracingCommandBus handle
 * </pre>
 * Each span is parented on the immediately enclosing span via the W3C trace context propagated on the command's
 * metadata across the gRPC boundary.
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
    void traceparentPropagatesAcrossAxonServerProducingTheFullFourSpanChain() {
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
        String busDispatch = "CommandBus.dispatch" + " " + commandName;
        String connectorDispatch = TracingCommandBusConnector.DISPATCH_SPAN + " " + commandName;
        String connectorHandle = TracingCommandBusConnector.HANDLE_SPAN + " " + commandName;
        String busHandle = "CommandBus.handle" + " " + commandName;

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> {
                   List<SpanData> spans = exporter.getFinishedSpanItems();
                   assertThat(spans).extracting(SpanData::getName)
                                    .contains(busDispatch, connectorDispatch, connectorHandle, busHandle);
               });

        List<SpanData> spans = exporter.getFinishedSpanItems();
        SpanData busDispatchSpan = spanByName(spans, busDispatch);
        SpanData connectorDispatchSpan = spanByName(spans, connectorDispatch);
        SpanData connectorHandleSpan = spanByName(spans, connectorHandle);
        SpanData busHandleSpan = spanByName(spans, busHandle);

        // span kinds
        assertThat(busDispatchSpan.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(connectorDispatchSpan.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(connectorHandleSpan.getKind()).isEqualTo(SpanKind.CONSUMER);
        assertThat(busHandleSpan.getKind()).isEqualTo(SpanKind.CONSUMER);

        // every span shares the same trace
        String traceId = busDispatchSpan.getTraceId();
        assertThat(connectorDispatchSpan.getTraceId()).isEqualTo(traceId);
        assertThat(connectorHandleSpan.getTraceId()).isEqualTo(traceId);
        assertThat(busHandleSpan.getTraceId()).isEqualTo(traceId);

        // parent chain: connectorDispatch parents on busDispatch (same JVM)
        assertThat(connectorDispatchSpan.getParentSpanContext().getSpanId())
                .as("connector-dispatch parents on bus-dispatch (in-process nesting)")
                .isEqualTo(busDispatchSpan.getSpanId());
        // connectorHandle parents on connectorDispatch (W3C traceparent across the gRPC hop)
        assertThat(connectorHandleSpan.getParentSpanContext().getSpanId())
                .as("connector-handle parents on connector-dispatch (traceparent across Axon Server)")
                .isEqualTo(connectorDispatchSpan.getSpanId());
        // busHandle parents on connectorHandle (the receive-leg span propagated its context onto the inbound command)
        assertThat(busHandleSpan.getParentSpanContext().getSpanId())
                .as("bus-handle parents on connector-handle (receive-leg propagated downstream)")
                .isEqualTo(connectorHandleSpan.getSpanId());
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
