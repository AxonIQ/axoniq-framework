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

package io.axoniq.framework.integrationtests.tracing;

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.MultiTenancyUtils;
import io.micrometer.context.ContextRegistry;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.configuration.MessagingTracingSettings;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for trace-context propagation into a <b>reactive</b> {@code @EventHandler} body: the handler
 * returns a {@link Flux} with operators, including a {@link Flux#publishOn(reactor.core.scheduler.Scheduler)} hop
 * onto a Reactor scheduler thread. With {@link Hooks#enableAutomaticContextPropagation()} active, Reactor captures
 * the thread-locals present at subscription -- through the {@code ObservationAwareSpanThreadLocalAccessor} the
 * thread-local bridge enhancer registers on the {@link ContextRegistry} -- and restores them around downstream
 * operator callbacks on other threads.
 * <p>
 * The capture point is the stream's subscription, which happens lazily when the handler's result stream is first
 * drained -- outside the synchronous handler invocation. The framework keeps the handler's span
 * thread-local-current around exactly that drain (the span-scoped pull window of {@code Span#runStream}), so the
 * capture sees the handler's own method-level span and every operator downstream -- whatever thread it runs on --
 * observes it as the current span.
 */
class ReactorContextPropagationTracingIntegrationTest {

    private static final String METHOD_SPAN = "ReactiveProjection.on(String)";

    private MicrometerTracingTestSetup tracing;
    private InMemorySpanExporter spanExporter;
    private @Nullable AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        tracing = MicrometerTracingTestSetup.create();
        spanExporter = tracing.spanExporter();
        Hooks.enableAutomaticContextPropagation();
    }

    @AfterEach
    void tearDown() {
        Hooks.disableAutomaticContextPropagation();
        if (configuration != null) {
            configuration.shutdown();
        }
        tracing.close();
    }

    @Test
    void reactorOperatorsOnAnotherThreadObserveTheHandlersSpanAsCurrentAndParentChildrenUnderIt() {
        // given a pooled-streaming processor whose handler body is a Flux with operators, hopping to a Reactor
        // scheduler thread via publishOn
        AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();
        ReactiveProjection projection = new ReactiveProjection(tracing.tracer());
        configuration = startApplication(eventSource, projection);
        // Resolving the ContextRegistry component applies the thread-local enhancer's decorator, guaranteeing the
        // span thread-local accessor is registered on the (global) registry before the Flux is subscribed.
        configuration.getComponent(ContextRegistry.class);

        // when the event is handled and the Flux completes on the Reactor scheduler thread
        eventSource.publishMessage(EventTestUtils.asEventMessage("probe"));
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(projection.operatorThread.get()).isNotNull());

        // then the operator ran on a Reactor scheduler thread, not the processor's worker thread
        assertThat(projection.operatorThread.get()).startsWith("boundedElastic");

        // and the Reactor hook restored the handler's method span as the thread-local current span there
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> {
                   SpanData methodSpan = spanNamed(METHOD_SPAN);
                   assertThat(projection.currentSpanInOperator.get()).isEqualTo(methodSpan.getSpanId());
               });

        // and a span created inside the operator on that thread parents under the handler's method span
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> {
                   SpanData methodSpan = spanNamed(METHOD_SPAN);
                   SpanData operatorChild = spanNamed("reactor-operator-child");
                   assertThat(operatorChild.getParentSpanContext().getSpanId()).isEqualTo(methodSpan.getSpanId());
               });
    }

    private AxonConfiguration startApplication(AsyncInMemoryStreamableEventSource eventSource,
                                               ReactiveProjection projection) {
        return MessagingConfigurer.create()
                                  // Not a multi-tenancy test. See MultiTenancyUtils#disable.
                                  .componentRegistry(MultiTenancyUtils::disable)
                                  .componentRegistry(registry -> registry
                                          // Stay local: no Axon Server connector.
                                          .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                          .registerComponent(SpanFactory.class, c -> tracing.spanFactory())
                                          .registerComponent(MessagingTracingSettings.class,
                                                             c -> MessagingTracingSettings.enabledByDefault())
                                          .registerComponent(Tracer.class, c -> tracing.tracer())
                                  )
                                  .eventProcessing(ep -> ep.pooledStreaming(
                                          ps -> ps.defaults(d -> d.eventSource(eventSource))
                                                  .defaultProcessor(
                                                          "psep-tracing-test-reactor-propagation-processor",
                                                          c -> c.autodetected("reactor-propagation",
                                                                              cfg -> projection)
                                                  )))
                                  .start();
    }

    private SpanData spanNamed(String name) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().equals(name))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError(
                                   "No span named '" + name + "'. Recorded: "
                                           + spanExporter.getFinishedSpanItems().stream()
                                                         .map(SpanData::getName)
                                                         .toList()));
    }

    /**
     * Reactive event handler: the body is a {@link Flux} pipeline with a {@code publishOn} thread hop; the operator
     * records the thread it ran on and the thread-local current span it observed there, and creates a child span
     * through the plain {@link Tracer} -- the way instrumented libraries would.
     */
    @SuppressWarnings("unused")
    static class ReactiveProjection {

        private final Tracer tracer;
        private final AtomicReference<@Nullable String> operatorThread = new AtomicReference<>();
        private final AtomicReference<@Nullable String> currentSpanInOperator = new AtomicReference<>();

        ReactiveProjection(Tracer tracer) {
            this.tracer = tracer;
        }

        @EventHandler
        public Flux<String> on(String event) {
            return Flux.just(event, event + "-follow-up")
                       .publishOn(Schedulers.boundedElastic())
                       .map(value -> {
                           if (operatorThread.get() == null) {
                               operatorThread.set(Thread.currentThread().getName());
                               io.micrometer.tracing.Span current = tracer.currentSpan();
                               currentSpanInOperator.set(current == null ? null : current.context().spanId());
                               io.micrometer.tracing.Span child = tracer.nextSpan()
                                                                        .name("reactor-operator-child")
                                                                        .start();
                               child.end();
                           }
                           return value;
                       });
        }
    }
}
