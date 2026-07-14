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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test proving the connector-level tracing decorators (axoniq-distributed-messaging) fire over a real
 * Axon Server transport. Dispatches a command through the configured CommandBus, which goes through the
 * {@code CommandBusConnector} on the way out and on the way in; the {@code TracingCommandBusConnector} decorator
 * emits the dispatch (send leg) and handle (receive leg) spans between the bus-level dispatch/handle spans.
 * <p>
 * Auto-skips when Docker is unavailable (delegates to {@link TracingAxonServerTestInfrastructure}).
 */
class ConnectorTracingAxonServerIT {

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
    void dispatchingThroughAxonServerProducesTheConnectorLevelSpans() {
        // given a configured CommandBus + a handler subscribed against the same Axon Server
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.spanExporter().reset();

        CommandHandlingModule commandHandlingModule =
                CommandHandlingModule.named("connector-tracing-slice")
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
                                      .orTimeout(30, TimeUnit.SECONDS)
                                      .join();

        // then both the connector-level send-leg and receive-leg spans show up in the same trace
        assertThat(result).isEqualTo("handled");

        InMemorySpanExporter exporter = INFRASTRUCTURE.spanExporter();
        String commandName = COMMAND_NAME.name();
        String connectorDispatch = TracingCommandBusConnector.DISPATCH_SPAN + " " + commandName;
        String connectorHandle = TracingCommandBusConnector.HANDLE_SPAN + " " + commandName;

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> {
                   List<SpanData> spans = exporter.getFinishedSpanItems();
                   assertThat(spans).extracting(SpanData::getName)
                                    .contains(connectorDispatch, connectorHandle);
               });
    }

    record GreetCommand(String who) {
    }
}
