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
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryUpdateEmitter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Distributed tracing integration tests over a real Axon Server boundary. Covers command propagation and the
 * subscription-query update parent/link relationship.
 */
class DistributedTracingAxonServerIT {

    private static final String COMMAND_CONNECTOR_DISPATCH_SPAN = "CommandBusConnector.dispatch";
    private static final String COMMAND_CONNECTOR_HANDLE_SPAN = "CommandBusConnector.handle";
    private static final String QUERY_CONNECTOR_SUBSCRIPTION_SPAN = "QueryBusConnector.subscriptionQuery";
    private static final String QUERY_CONNECTOR_UPDATE_SPAN = "QueryBusConnector.queryUpdate";
    private static final QualifiedName COMMAND_NAME = new QualifiedName(GreetCommand.class);
    private static final QualifiedName QUERY_NAME = new QualifiedName("RoomSubscription");

    private TracingAxonServerTestInfrastructure infrastructure;
    private AxonConfiguration startedConfiguration;
    private boolean infrastructureStarted;

    @BeforeEach
    void setUp() {
        infrastructure = new TracingAxonServerTestInfrastructure();
    }

    @AfterEach
    void tearDown() {
        try {
            if (startedConfiguration != null) {
                startedConfiguration.shutdown();
            }
        } finally {
            if (infrastructureStarted) {
                infrastructure.stop();
            }
        }
    }

    @Test
    void commandTraceContextPropagatesAcrossAxonServer() {
        // given
        startInfrastructure();
        CommandHandlingModule commandHandlingModule =
                CommandHandlingModule.named("distributed-tracing-command-module")
                                     .commandHandlers()
                                     .commandHandler(
                                             COMMAND_NAME,
                                             (command, context) -> MessageStream.just(new GenericCommandResultMessage(
                                                     new MessageType("greeting"), "handled"
                                             ))
                                     )
                                     .build();
        startedConfiguration = MessagingConfigurer.create()
                                                  .registerCommandHandlingModule(() -> commandHandlingModule)
                                                  .componentRegistry(infrastructure::configureInfrastructure)
                                                  .start();

        // when
        String result = startedConfiguration.getComponent(CommandGateway.class)
                                            .send(new GreetCommand("world"))
                                            .resultAs(String.class)
                                            .orTimeout(30, TimeUnit.SECONDS)
                                            .join();

        // then
        assertThat(result).isEqualTo("handled");
        String commandName = COMMAND_NAME.name();
        String busDispatch = "CommandBus.dispatch " + commandName;
        String connectorDispatch = COMMAND_CONNECTOR_DISPATCH_SPAN + " " + commandName;
        String connectorHandle = COMMAND_CONNECTOR_HANDLE_SPAN + " " + commandName;
        String busHandle = "CommandBus.handle " + commandName;
        InMemorySpanExporter exporter = infrastructure.spanExporter();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(exporter.getFinishedSpanItems())
                        .extracting(SpanData::getName)
                        .contains(busDispatch, connectorDispatch, connectorHandle, busHandle)
        );

        List<SpanData> spans = exporter.getFinishedSpanItems();
        SpanData busDispatchSpan = spanByName(spans, busDispatch);
        SpanData connectorDispatchSpan = spanByName(spans, connectorDispatch);
        SpanData connectorHandleSpan = spanByName(spans, connectorHandle);
        SpanData busHandleSpan = spanByName(spans, busHandle);

        assertThat(busDispatchSpan.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(connectorDispatchSpan.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(connectorHandleSpan.getKind()).isEqualTo(SpanKind.CONSUMER);
        assertThat(busHandleSpan.getKind()).isEqualTo(SpanKind.CONSUMER);

        assertThat(connectorDispatchSpan.getParentSpanId()).isEqualTo(busDispatchSpan.getSpanId());
        assertThat(connectorHandleSpan.getParentSpanId()).isEqualTo(connectorDispatchSpan.getSpanId());
        assertThat(busHandleSpan.getParentSpanId()).isEqualTo(connectorHandleSpan.getSpanId());
        assertThat(spansByName(spans, busDispatch, connectorDispatch, connectorHandle, busHandle))
                .extracting(SpanData::getTraceId)
                .containsOnly(busDispatchSpan.getTraceId());
    }

    @Test
    void subscriptionQueryUpdateIsParentedOnTheEmitterAndLinkedToTheSubscription() throws InterruptedException {
        // given
        startInfrastructure();
        ApplicationConfigurer configurer = MessagingConfigurer.create()
                                                              .componentRegistry(
                                                                      infrastructure::configureInfrastructure
                                                              );
        startedConfiguration = configurer.start();
        QueryBus queryBus = startedConfiguration.getComponent(QueryBus.class);
        CountDownLatch queryHandled = new CountDownLatch(1);
        AtomicReference<QueryUpdateEmitter> emitter = new AtomicReference<>();
        queryBus.subscribe(QUERY_NAME, (query, context) -> {
            emitter.set(QueryUpdateEmitter.forContext(context));
            queryHandled.countDown();
            return MessageStream.just(
                    new GenericQueryResponseMessage(new MessageType("RoomInitial"), "room-42 available")
            );
        });

        MessageStream<QueryResponseMessage> result = queryBus.subscriptionQuery(
                new GenericQueryMessage(new MessageType(QUERY_NAME.fullName()), "room-42"),
                null,
                16
        );
        assertThat(queryHandled.await(30, TimeUnit.SECONDS)).isTrue();
        await().atMost(Duration.ofSeconds(30)).until(result::hasNextAvailable);
        assertThat(result.next().orElseThrow().message().payloadAs(String.class)).isEqualTo("room-42 available");

        // when
        emitter.get().emit(
                QUERY_NAME,
                query -> true,
                () -> new GenericSubscriptionQueryUpdateMessage(
                        new MessageType("RoomUpdate"),
                        "room-42 booked"
                )
        );
        await().atMost(Duration.ofSeconds(30)).until(result::hasNextAvailable);
        assertThat(result.next().orElseThrow().message().payloadAs(String.class)).isEqualTo("room-42 booked");
        emitter.get().complete(QUERY_NAME, query -> true);

        // then
        InMemorySpanExporter exporter = infrastructure.spanExporter();
        String subscriptionSpanName = "QueryBus.subscriptionQuery " + QUERY_NAME.name();
        String connectorSubscriptionSpanName = QUERY_CONNECTOR_SUBSCRIPTION_SPAN + " " + QUERY_NAME.name();
        String updateSpanName = QUERY_CONNECTOR_UPDATE_SPAN + " RoomUpdate";
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(exporter.getFinishedSpanItems())
                        .extracting(SpanData::getName)
                        .contains(
                                "QueryBus.emitUpdate",
                                subscriptionSpanName,
                                connectorSubscriptionSpanName,
                                updateSpanName
                        )
        );

        List<SpanData> spans = exporter.getFinishedSpanItems();
        SpanData subscription = spanByNameAndKind(spans, subscriptionSpanName, SpanKind.PRODUCER);
        SpanData producer = spanByNameAndKind(spans, updateSpanName, SpanKind.PRODUCER);
        SpanData consumer = spanByNameAndKind(spans, updateSpanName, SpanKind.CONSUMER);

        assertThat(consumer.getTraceId()).isEqualTo(producer.getTraceId());
        assertThat(consumer.getParentSpanId()).isEqualTo(producer.getSpanId());
        assertThat(consumer.getLinks()).anySatisfy(link -> {
            assertThat(link.getSpanContext().getTraceId()).isEqualTo(subscription.getTraceId());
            assertThat(link.getSpanContext().getSpanId()).isEqualTo(subscription.getSpanId());
        });
    }

    private void startInfrastructure() {
        infrastructure.start();
        infrastructureStarted = true;
        infrastructure.spanExporter().reset();
    }

    private static List<SpanData> spansByName(List<SpanData> spans, String... names) {
        List<String> expectedNames = List.of(names);
        return spans.stream().filter(span -> expectedNames.contains(span.getName())).toList();
    }

    private static SpanData spanByName(List<SpanData> spans, String name) {
        return spans.stream()
                    .filter(span -> span.getName().equals(name))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No span named " + name));
    }

    private static SpanData spanByNameAndKind(List<SpanData> spans, String name, SpanKind kind) {
        return spans.stream()
                    .filter(span -> span.getName().equals(name))
                    .filter(span -> span.getKind() == kind)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No " + kind + " span named " + name));
    }

    private record GreetCommand(String who) {
    }
}
