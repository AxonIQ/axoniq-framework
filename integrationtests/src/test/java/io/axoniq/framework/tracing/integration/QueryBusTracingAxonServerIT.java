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

import org.axonframework.messaging.queryhandling.tracing.TracingQueryBus;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test proving W3C trace-context propagates across the Axon Server gRPC boundary on the query side.
 * Mirrors {@link CommandBusTracingAxonServerIT}: one application dispatches a query to itself through Axon Server,
 * and we assert the bus-level dispatch + handle spans share a trace.
 * <p>
 * NB: AF5's distributed query path on Axon Server does not currently go through a registered
 * {@link io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector} component slot the way the command
 * path does, so the {@code QueryBusConnector.query} / {@code QueryBusConnector.handle} connector spans are not
 * asserted here. The connector decorator itself is exercised by {@code TracingCommandBusConnectorTest} (commands) and
 * the connector tracing module's unit tests; this IT focuses on the bus-level continuity across the gRPC hop.
 * <p>
 * Auto-skips when Docker is unavailable (Testcontainers built-in check via {@link TracingAxonServerTestInfrastructure}).
 */
class QueryBusTracingAxonServerIT {

    private static final TracingAxonServerTestInfrastructure INFRASTRUCTURE =
            new TracingAxonServerTestInfrastructure();

    private static final QualifiedName QUERY_NAME = new QualifiedName(FindGreeting.class);

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
    void traceparentPropagatesAcrossAxonServerSoQueryDispatchAndHandleShareATrace() {
        // given
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.spanExporter().reset();

        QueryHandlingModule queryHandlingModule =
                QueryHandlingModule.named("tracing-query-slice")
                                   .queryHandlers()
                                   .queryHandler(
                                           QUERY_NAME,
                                           (query, context) -> MessageStream.just(new GenericQueryResponseMessage(
                                                   new MessageType("greeting"), "hello-world"
                                           ))
                                   )
                                   .build();

        ApplicationConfigurer configurer =
                MessagingConfigurer.create()
                                   .registerQueryHandlingModule(() -> queryHandlingModule)
                                   .componentRegistry(INFRASTRUCTURE::configureInfrastructure);
        startedConfiguration = configurer.start();
        QueryGateway queryGateway = startedConfiguration.getComponent(QueryGateway.class);

        // when
        String result = queryGateway.query(new FindGreeting("world"), String.class, null)
                                    .orTimeout(30, TimeUnit.SECONDS)
                                    .join();

        // then
        assertThat(result).isEqualTo("hello-world");

        InMemorySpanExporter exporter = INFRASTRUCTURE.spanExporter();
        String queryName = QUERY_NAME.name();
        String dispatchSpanName = TracingQueryBus.DISPATCH_SPAN + " " + queryName;
        String handleSpanName = TracingQueryBus.HANDLE_SPAN + " " + queryName;

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

        // The key assertion: the traceparent rode across the Axon Server gRPC boundary on the query metadata.
        assertThat(handleSpan.getTraceId())
                .as("handle span trace id must equal dispatch span trace id (same trace across Axon Server)")
                .isEqualTo(dispatchSpan.getTraceId());
    }

    private static SpanData spanByName(List<SpanData> spans, String name) {
        Optional<SpanData> match = spans.stream().filter(span -> span.getName().equals(name)).findFirst();
        assertThat(match).as("expected a span named '%s'", name).isPresent();
        return match.get();
    }

    record FindGreeting(String who) {
    }
}
