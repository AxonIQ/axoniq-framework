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
import org.axonframework.messaging.queryhandling.tracing.TracingQueryBus;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.FluxUtils;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for subscription-query update tracing through the real configuration: a
 * {@link MessagingConfigurer} registers a Micrometer {@code SpanFactory}, the ServiceLoader-discovered
 * {@code MessagingTracingConfigurationEnhancer} decorates the {@link QueryBus}, and the collapsed
 * query-update-emitter operations ({@code subscriptionQuery} / {@code emitUpdate} / {@code completeSubscriptions})
 * must each produce their span. With no {@link SpanFactory} registered, the same flow produces no spans.
 */
class QueryUpdateTracingConfigurationIntegrationTest {

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
    void aSubscriptionQueryWithUpdatesProducesTheDispatchEmitAndCompleteSpans() {
        // given a configured application with an annotated query handler and the OTel SpanFactory registered
        configuration = startApplication(/* registerSpanFactory */ true);
        QueryBus queryBus = configuration.getComponent(QueryBus.class);

        // when a subscription query is dispatched, an update is emitted and the subscription is completed
        QueryMessage query = new GenericQueryMessage(new MessageType(FindRoom.class), new FindRoom("room-42"));
        MessageStream<QueryResponseMessage> result = queryBus.subscriptionQuery(query, null, 50);

        // Nested classes' QualifiedName is package + simple name (no enclosing-class prefix), unlike Class#getName().
        QualifiedName findRoomName = new QualifiedName(FindRoom.class);
        Predicate<QueryMessage> matchingSubscription =
                q -> findRoomName.equals(q.type().qualifiedName());
        SubscriptionQueryUpdateMessage update =
                new GenericSubscriptionQueryUpdateMessage(new MessageType("RoomUpdate"), "room-42-updated");
        queryBus.emitUpdate(matchingSubscription, () -> update, null)
                .orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();
        queryBus.completeSubscriptions(matchingSubscription, null)
                .orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();

        // then the initial result and the update are delivered
        StepVerifier.create(FluxUtils.of(result)
                                     .map(MessageStream.Entry::message)
                                     .mapNotNull(message -> message.payloadAs(String.class)))
                    .expectNext("room-42-info", "room-42-updated")
                    .verifyComplete();

        // and the dispatch, emit and complete spans are exported
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(spanNames())
                .anyMatch(name -> name.startsWith(TracingQueryBus.SUBSCRIPTION_QUERY_SPAN))
                .contains(TracingQueryBus.EMIT_UPDATE_SPAN, TracingQueryBus.COMPLETE_SUBSCRIPTIONS_SPAN));

        SpanData subscriptionSpan = spanStartingWith(TracingQueryBus.SUBSCRIPTION_QUERY_SPAN);
        SpanData emitSpan = spanNamed(TracingQueryBus.EMIT_UPDATE_SPAN);
        SpanData completeSpan = spanNamed(TracingQueryBus.COMPLETE_SUBSCRIPTIONS_SPAN);
        assertThat(subscriptionSpan.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(emitSpan.getKind()).isEqualTo(SpanKind.INTERNAL);
        assertThat(completeSpan.getKind()).isEqualTo(SpanKind.INTERNAL);
    }

    @Test
    void withoutAConfiguredSpanFactoryNoTracingSpansAreProduced() {
        // given the same configuration but WITHOUT a SpanFactory registered
        configuration = startApplication(/* registerSpanFactory */ false);
        QueryBus queryBus = configuration.getComponent(QueryBus.class);

        // when
        QueryMessage query = new GenericQueryMessage(new MessageType(FindRoom.class), new FindRoom("room-42"));
        MessageStream<QueryResponseMessage> result = queryBus.subscriptionQuery(query, null, 50);
        // Nested classes' QualifiedName is package + simple name (no enclosing-class prefix), unlike Class#getName().
        QualifiedName findRoomName = new QualifiedName(FindRoom.class);
        Predicate<QueryMessage> matchingSubscription =
                q -> findRoomName.equals(q.type().qualifiedName());
        queryBus.completeSubscriptions(matchingSubscription, null)
                .orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();

        // then the initial result is delivered but the InMemorySpanExporter has nothing — tracing is fully off
        StepVerifier.create(FluxUtils.of(result)
                                     .map(MessageStream.Entry::message)
                                     .mapNotNull(message -> message.payloadAs(String.class)))
                    .expectNext("room-42-info")
                    .verifyComplete();
        assertThat(spanExporter.getFinishedSpanItems()).isEmpty();
    }

    private AxonConfiguration startApplication(boolean registerSpanFactory) {
        QueryHandlingModule queryHandlingModule =
                QueryHandlingModule.named("tracing-update-test")
                                   .queryHandlers()
                                   .autodetectedQueryHandlingComponent(c -> new FindRoomHandler())
                                   .build();
        MessagingConfigurer configurer = MessagingConfigurer.create()
                                                            .componentRegistry(registry -> registry
                                                                    // Stay local: the Axon Server connector would
                                                                    // otherwise flip the bus to its distributed shape.
                                                                    .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                                            )
                                                            .registerQueryHandlingModule(() -> queryHandlingModule);
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
                           .orElseThrow(() -> new AssertionError(
                                   "No span starting with " + prefix + " in " + spanNames()));
    }

    @SuppressWarnings("unused")
    record FindRoom(String roomId) {
    }

    @SuppressWarnings("unused")
    static class FindRoomHandler {

        @QueryHandler
        public String handle(FindRoom query) {
            return query.roomId() + "-info";
        }
    }
}
