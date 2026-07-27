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

import io.axoniq.framework.messaging.queryhandling.distributed.tracing.TracingQueryBusConnector;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryUpdateEmitter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test proving the complete subscription-query update path is traced over a real Axon Server hop. It
 * verifies the bus-level emit span, the connector producer and consumer spans, their cross-hop parent edge, and the
 * consumer span's link back to the originating subscription query.
 * <p>
 * Auto-skips when Docker is unavailable.
 */
class QueryUpdateEmitterTracingAxonServerIT {

    private static final TracingAxonServerTestInfrastructure INFRASTRUCTURE =
            new TracingAxonServerTestInfrastructure();
    private static final QualifiedName QUERY_NAME = new QualifiedName("RoomSubscription");

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
    void updateThroughAxonServerIsParentedOnTheEmitterAndLinkedToTheSubscription() throws InterruptedException {
        // given a distributed subscription query on Axon Server
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.spanExporter().reset();

        ApplicationConfigurer configurer =
                MessagingConfigurer.create()
                                   .componentRegistry(INFRASTRUCTURE::configureInfrastructure);
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

        // when an update crosses the connector and is delivered to the subscriber
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

        // then the connector delivery is a child of its producer and links back to the subscription dispatch
        InMemorySpanExporter exporter = INFRASTRUCTURE.spanExporter();
        String originatingQuerySpanName = "QueryBus.subscriptionQuery" + " " + QUERY_NAME.name();
        String connectorSubscriptionSpanName =
                TracingQueryBusConnector.SUBSCRIPTION_QUERY_SPAN + " " + QUERY_NAME.name();
        String updateSpanName = TracingQueryBusConnector.QUERY_UPDATE_SPAN + " RoomUpdate";
        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> {
                   List<SpanData> spans = exporter.getFinishedSpanItems();
                   assertThat(spans).extracting(SpanData::getName)
                                    .contains("QueryBus.emitUpdate",
                                              originatingQuerySpanName,
                                              connectorSubscriptionSpanName,
                                              updateSpanName);
               });

        List<SpanData> spans = exporter.getFinishedSpanItems();
        SpanData subscription = spanByNameAndKind(spans, originatingQuerySpanName, SpanKind.PRODUCER);
        SpanData producer = spanByNameAndKind(spans, updateSpanName, SpanKind.PRODUCER);
        SpanData consumer = spanByNameAndKind(spans, updateSpanName, SpanKind.CONSUMER);

        assertThat(consumer.getTraceId()).isEqualTo(producer.getTraceId());
        assertThat(consumer.getParentSpanId()).isEqualTo(producer.getSpanId());
        assertThat(consumer.getLinks())
                .anySatisfy(link -> {
                    assertThat(link.getSpanContext().getTraceId()).isEqualTo(subscription.getTraceId());
                    assertThat(link.getSpanContext().getSpanId()).isEqualTo(subscription.getSpanId());
                });
    }

    private static SpanData spanByNameAndKind(List<SpanData> spans, String name, SpanKind kind) {
        return spans.stream()
                    .filter(span -> span.getName().equals(name))
                    .filter(span -> span.getKind() == kind)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No " + kind + " span named " + name));
    }
}
