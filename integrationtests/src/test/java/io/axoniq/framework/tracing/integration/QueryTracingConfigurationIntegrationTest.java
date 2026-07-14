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
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
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
 * Integration test for query-side tracing through the real configuration: a {@link MessagingConfigurer} registers an
 * a Micrometer {@code SpanFactory} as a component, disables the Axon Server connector enhancer so the bus stays local,
 * and registers an annotated {@code @QueryHandler}. Dispatching through the {@link QueryGateway} must produce the full
 * nested span tree {@code QueryBus.query} (PRODUCER) → {@code QueryBus.handleQuery} (CONSUMER) →
 * {@code FindRoomHandler.handle(FindRoom)} (INTERNAL), with no manual decorator or handler-definition wiring (per
 * constitution §Testing). With no {@link SpanFactory} registered, the same wiring produces no spans.
 */
class QueryTracingConfigurationIntegrationTest {

    private static final String ENHANCER_SPAN = "FindRoomHandler.handle(FindRoom)";
    private static final String HANDLE_SPAN_PREFIX = "QueryBus.handleQuery";
    private static final String DISPATCH_SPAN_PREFIX = "QueryBus.query";

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
    void aDispatchedQueryProducesTheFullSpanTreeThroughTheConfigurationDrivenWiring() {
        // given a configured application that registers a Micrometer SpanFactory and an annotated query handler,
        // with the Axon Server connector disabled so the query bus stays local (no Docker required)
        configuration = startApplication(/* registerSpanFactory */ true);
        QueryGateway queryGateway = configuration.getComponent(QueryGateway.class);

        // when a query is dispatched
        String result = queryGateway.query(new FindRoom("room-42"), String.class, null)
                                    .orTimeout(30, TimeUnit.SECONDS)
                                    .join();

        // then the dispatch + handle + per-method enhancer spans appear, all sharing one trace, properly nested:
        // dispatch -> handle -> FindRoomHandler.handle(FindRoom)
        assertThat(result).isEqualTo("room-42-info");
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
        // given the same configuration but WITHOUT a SpanFactory registered
        configuration = startApplication(/* registerSpanFactory */ false);
        QueryGateway queryGateway = configuration.getComponent(QueryGateway.class);

        // when
        String result = queryGateway.query(new FindRoom("room-42"), String.class, null)
                                    .orTimeout(30, TimeUnit.SECONDS)
                                    .join();

        // then the query is handled normally, but the InMemorySpanExporter has nothing — tracing is fully off
        assertThat(result).isEqualTo("room-42-info");
        assertThat(spanExporter.getFinishedSpanItems()).isEmpty();
    }

    private AxonConfiguration startApplication(boolean registerSpanFactory) {
        QueryHandlingModule queryHandlingModule =
                QueryHandlingModule.named("tracing-config-test")
                                   .queryHandlers()
                                   .autodetectedQueryHandlingComponent(c -> new FindRoomHandler())
                                   .build();
        MessagingConfigurer configurer = MessagingConfigurer.create()
                                                            .componentRegistry(registry -> registry
                                                                    .disableEnhancer(
                                                                            AxonServerConfigurationEnhancer.class))
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

    private record FindRoom(String roomId) {

    }

    @SuppressWarnings("unused")
    static class FindRoomHandler {

        @QueryHandler
        public String handle(FindRoom query) {
            return query.roomId() + "-info";
        }
    }
}
