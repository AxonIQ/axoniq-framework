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
import io.micrometer.tracing.Tracer;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.MessagingTracingSettings;
import org.axonframework.messaging.eventhandling.tracing.TracingEventHandlingComponent;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandDispatcher;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the streaming-processor batch span and the event-processor sub-toggles through the real
 * configuration: a {@link MessagingConfigurer} wires a {@code PooledStreamingEventProcessor} against an in-memory
 * {@link AsyncInMemoryStreamableEventSource}, tracing arrives via the ServiceLoader-discovered enhancer, and the
 * {@link MessagingTracingSettings} component carries the {@code disableBatchTrace} / {@code distributedInSameTrace}
 * toggles exactly as the Spring autoconfiguration registers them. Asserts:
 * <ul>
 *     <li>{@code distributedInSameTrace=true} — no batch root span is produced (AF4 parity: per-event spans continue
 *     their publishers' traces, so a batch root would dangle);</li>
 *     <li>{@code disableBatchTrace=true} with same-trace mode off -- no batch span, and the per-event span is a
 *     linked root;</li>
 *     <li>default settings ({@code distributedInSameTrace=false}) -- the per-event span is parented to the batch and
 *     linked to the publisher.</li>
 * </ul>
 */
class PooledStreamingEventTracingConfigurationIntegrationTest {

    private static final String BATCH_SPAN = TracingEventHandlingComponent.BATCH_SPAN;
    private static final String PROCESS_SPAN_PREFIX = TracingEventHandlingComponent.PROCESS_SPAN;

    private MicrometerTracingTestSetup tracing;
    private InMemorySpanExporter spanExporter;
    private SpanFactory spanFactory;
    private @Nullable AxonConfiguration configuration;
    private @Nullable ScheduledExecutorService mixedBatchWorkerExecutor;

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
        if (mixedBatchWorkerExecutor != null) {
            mixedBatchWorkerExecutor.shutdownNow();
        }
        tracing.close();
    }

    @Test
    void distributedInSameTraceModeSuppressesTheBatchRootSpan() {
        // given a pooled-streaming processor with same-trace mode enabled (distributedInSameTrace = true)
        AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();
        BookingProjection projection = new BookingProjection();
        configuration = startApplication(eventSource, projection, settings(/* disableBatchTrace */ false,
                                                                           /* distributedInSameTrace */ true));

        // when an event is streamed through the processor
        EventMessage event = EventTestUtils.asEventMessage("room-42");
        eventSource.publishMessage(event);
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(projection.handledEvent.get()).isEqualTo("room-42"));

        // then the per-event handler span fires, but no batch root span is produced (AF4 parity: in same-trace mode
        // per-event spans continue their publishers' traces, so a batch root would dangle without children)
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(spanNames()).anyMatch(name -> name.startsWith(PROCESS_SPAN_PREFIX)));
        assertThat(spanNames()).noneMatch(name -> name.equals(BATCH_SPAN));
    }

    @Test
    void disableBatchTraceLeavesTheEventHandlerAsALinkedRootSpan() {
        // given a pooled-streaming processor with batch tracing disabled and same-trace mode off
        AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();
        BookingProjection projection = new BookingProjection();
        configuration = startApplication(eventSource, projection, settings(/* disableBatchTrace */ true,
                                                                           /* distributedInSameTrace */ false));

        // when
        EventMessage event = EventTestUtils.asEventMessage("room-42");
        eventSource.publishMessage(event);
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(projection.handledEvent.get()).isEqualTo("room-42"));

        // then the per-event handler span is a root and no batch span is produced
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(spanNames()).anyMatch(name -> name.startsWith(PROCESS_SPAN_PREFIX)));
        assertThat(spanNames()).noneMatch(name -> name.equals(BATCH_SPAN));
        assertThat(SpanId.isValid(spanStartingWith(PROCESS_SPAN_PREFIX).getParentSpanId())).isFalse();
    }

    @Test
    void perEventSpanJoinsTheBatchTraceByDefault() {
        // given a pooled-streaming processor with the default settings (distributedInSameTrace = false)
        AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();
        BookingProjection projection = new BookingProjection();
        configuration = startApplication(eventSource, projection, MessagingTracingSettings.enabledByDefault());

        // when
        EventMessage event = EventTestUtils.asEventMessage("room-42");
        eventSource.publishMessage(event);
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(projection.handledEvent.get()).isEqualTo("room-42"));

        // then the batch is the per-event span's structural parent
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(spanNames()).anyMatch(name -> name.startsWith(PROCESS_SPAN_PREFIX)));
        SpanData batchSpan = spanNamed(BATCH_SPAN);
        SpanData processSpan = spanStartingWith(PROCESS_SPAN_PREFIX);
        assertThat(processSpan.getParentSpanContext().getSpanId()).isEqualTo(batchSpan.getSpanId());
        assertThat(processSpan.getTraceId()).isEqualTo(batchSpan.getTraceId());

        // and both processor-owned spans identify the owning processor by name, through the real enhancer wiring
        AttributeKey<String> processorNameKey =
                AttributeKey.stringKey(TracingEventHandlingComponent.PROCESSOR_NAME_ATTRIBUTE);
        assertThat(batchSpan.getAttributes().get(processorNameKey)).isEqualTo("psep-tracing-test-processor");
        assertThat(processSpan.getAttributes().get(processorNameKey)).isEqualTo("psep-tracing-test-processor");
    }

    /**
     * Regression test for the per-event active-span collapse fix: a real, single streaming batch containing two
     * events, each doing further tracing-relevant work of its own (a command dispatch; a follow-up event append),
     * asserts every child parents under its <em>own</em> event's spans -- not the other event's, and not a stale
     * last-writer -- and that per-event spans close well before the shared batch span.
     * <p>
     * Both events go through the framework's default (single) sequencing policy, i.e. the same
     * {@code SequencingEventHandlingComponent} chain within one batch context -- the scenario the collapse bug
     * actually manifested in.
     * <p>
     * Not covered here (verified instead by the OSS unit regression suite in
     * {@code TracingEventHandlingComponentTest.BranchCarriedSpans}): a framework-triggered flush during the UnitOfWork's
     * COMMIT phase honestly attaching to the batch span. Reproducing that end-to-end would need a real storage engine
     * driving a {@code ProcessingLifecycleInterceptor}-level span, which this lightweight in-memory harness has no
     * equivalent for. The in-body thread-local view ({@code Tracer#currentSpan()} inside a real handler body) is
     * covered by {@code handlerBodyObservesItsOwnMethodSpanAsTheTracersCurrentSpan} below.
     */
    @Test
    void aTwoEventSingleBatchEachEventsChildSpansParentUnderItsOwnEventSpanNotTheOthers() {
        // given a streaming batch forced to size >= 2 (the framework default is 1) with both events published
        // BEFORE the processor starts, so both are already available for its very first fetch. Publishing before
        // start is necessary but not sufficient: the Coordinator's synchronous per-tick fetch loop drains every
        // already-available event into the WorkPackage's queue in one pass, but the WorkPackage's OWN worker is
        // submitted to a separate executor as soon as the FIRST event is scheduled and could race ahead and start
        // polling before the second event is enqueued, splitting the two across separate batches. A worker executor
        // that delays actually running the polling task closes that race deterministically.
        mixedBatchWorkerExecutor = delayedWorkerExecutor();
        AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();
        MixedBatchProjection projection = new MixedBatchProjection();
        EventMessage event1 = EventTestUtils.asEventMessage(new BookingRequested("room-1"));
        EventMessage event2 = EventTestUtils.asEventMessage(new BookingConfirmed("room-1"));
        eventSource.publishMessage(event1);
        eventSource.publishMessage(event2);

        configuration = startMixedBatchApplication(eventSource, projection,
                                                   settings(/* disableBatchTrace */ false,
                                                            /* distributedInSameTrace */ false));

        // when both events are handled, in one batch
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(projection.firstHandled.get()).isEqualTo("room-1"));
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(projection.secondHandled.get()).isEqualTo("room-1"));

        String processSpan1Name = PROCESS_SPAN_PREFIX + " " + event1.type().qualifiedName().name();
        String processSpan2Name = PROCESS_SPAN_PREFIX + " " + event2.type().qualifiedName().name();
        String methodSpan1Name = "MixedBatchProjection.on(BookingRequested,CommandDispatcher)";
        String methodSpan2Name = "MixedBatchProjection.on(BookingConfirmed,EventAppender)";
        String publishSpanName = "EventSink.publish "
                + EventTestUtils.asEventMessage(new AuditLogged("room-1")).type().qualifiedName().name();
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(spanNames()).contains(
                       BATCH_SPAN, processSpan1Name, processSpan2Name, methodSpan1Name, methodSpan2Name,
                       publishSpanName
               ));

        // then each event's handler span is a distinct child of the shared batch span
        SpanData batchSpan = spanNamed(BATCH_SPAN);
        SpanData processSpan1 = spanNamed(processSpan1Name);
        SpanData processSpan2 = spanNamed(processSpan2Name);
        assertThat(processSpan1.getParentSpanContext().getSpanId()).isEqualTo(batchSpan.getSpanId());
        assertThat(processSpan2.getParentSpanContext().getSpanId()).isEqualTo(batchSpan.getSpanId());
        assertThat(processSpan1.getTraceId()).isEqualTo(batchSpan.getTraceId());
        assertThat(processSpan2.getTraceId()).isEqualTo(batchSpan.getTraceId());
        assertThat(processSpan1.getSpanId()).isNotEqualTo(processSpan2.getSpanId());

        // each event's method span nests directly under its own event's process span (batch -> process -> method)
        SpanData methodSpan1 = spanNamed(methodSpan1Name);
        SpanData methodSpan2 = spanNamed(methodSpan2Name);
        assertThat(methodSpan1.getParentSpanContext().getSpanId()).isEqualTo(processSpan1.getSpanId());
        assertThat(methodSpan2.getParentSpanContext().getSpanId()).isEqualTo(processSpan2.getSpanId());

        // the command dispatched from event 1's handler nests under event 1's own method span -- the dispatch span
        // resolves its parent from the branched context, not from the enclosing batch or (worse) event 2's span
        SpanData dispatchSpan = spanStartingWith("CommandBus.dispatchCommand");
        assertThat(dispatchSpan.getParentSpanContext().getSpanId()).isEqualTo(methodSpan1.getSpanId());

        // the follow-up event appended by event 2 produces its own EventSink span under event 2's method span --
        // it does not inherit whichever sibling event happened to be active last
        SpanData publishSpan = spanNamed(publishSpanName);
        assertThat(publishSpan.getParentSpanContext().getSpanId()).isEqualTo(methodSpan2.getSpanId());
        assertThat(spanNames()).noneMatch(name -> name.startsWith("EventBus.commitEvents"));

        // per-event handler spans close on their own stream's termination, well before the batch (and the whole
        // unit of work) completes
        assertThat(processSpan1.getEndEpochNanos()).isLessThan(batchSpan.getEndEpochNanos());
        assertThat(processSpan2.getEndEpochNanos()).isLessThan(batchSpan.getEndEpochNanos());
    }

    private AxonConfiguration startMixedBatchApplication(AsyncInMemoryStreamableEventSource eventSource,
                                                         Object projection,
                                                         MessagingTracingSettings settings) {
        CommandHandlingModule commandHandlingModule =
                CommandHandlingModule.named("pooled-streaming-tracing-test")
                                     .commandHandlers()
                                     .autodetectedCommandHandlingComponent(c -> new ConfirmBookingHandler())
                                     .build();
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> registry
                                          // Stay local: no Axon Server connector.
                                          .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                          .registerComponent(SpanFactory.class, c -> spanFactory)
                                          .registerComponent(MessagingTracingSettings.class, c -> settings)
                                          // Required by the auto-discovered thread-local context-propagation
                                          // enhancer (io.axoniq.framework.tracing.micrometer.threadlocal), which
                                          // reads the Tracer component regardless of this test's own SpanFactory
                                          // wiring.
                                          .registerComponent(Tracer.class, c -> tracing.tracer())
                                  )
                                  .registerCommandHandlingModule(() -> commandHandlingModule)
                                  .eventProcessing(ep -> ep.pooledStreaming(
                                          // A single segment (default is 16) so both events are claimed by the SAME
                                          // work package, batchSize >= 2 (default is 1) so they land in ONE batch, and
                                          // a deliberately-delayed worker executor so the coordinator's synchronous
                                          // per-tick fetch loop always wins the race described above.
                                          ps -> ps.defaults(d -> d.eventSource(eventSource)
                                                                  .initialSegmentCount(1)
                                                                  .batchSize(2)
                                                                  .workerExecutor(() -> mixedBatchWorkerExecutor))
                                                  .defaultProcessor(
                                                          "psep-tracing-test-mixed-batch-processor",
                                                          c -> c.autodetected("mixed-batch", cfg -> projection)
                                                  )
                                  ))
                                  .start();
    }

    /**
     * A {@link ScheduledExecutorService} that delays actually running a submitted {@code WorkPackage} worker task
     * (the only overload {@code WorkPackage.scheduleWorker()} calls), so the {@code Coordinator}'s own synchronous
     * per-tick fetch loop -- which drains every already-published event into the work package's queue in one pass --
     * always finishes first. Without this, the worker executor could win the race and start polling the queue after
     * only the first event was enqueued, splitting two already-published events across separate batches.
     */
    private static ScheduledExecutorService delayedWorkerExecutor() {
        return new ScheduledThreadPoolExecutor(2) {
            @Override
            public Future<?> submit(Runnable task) {
                return schedule(task, 50, TimeUnit.MILLISECONDS);
            }
        };
    }

    @Test
    void handlerBodyObservesItsOwnMethodSpanAsTheTracersCurrentSpan() {
        // given a pooled-streaming processor whose handler records Tracer#currentSpan() from inside the body --
        // the view instrumented libraries (datasource-micrometer JDBC, WebClient) and user tracer calls read
        AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();
        CurrentSpanProbeProjection projection = new CurrentSpanProbeProjection(tracing.tracer());
        configuration = startProbeApplication(eventSource, projection);

        // when an event is handled through the full annotation/interceptor pipeline
        eventSource.publishMessage(EventTestUtils.asEventMessage("probe"));
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(projection.handled.get()).isTrue());

        // then the thread-local current span inside the body is exactly this handler's (innermost) method-level span
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> {
                   SpanData methodSpan = spanNamed("CurrentSpanProbeProjection.on(String)");
                   assertThat(projection.currentSpanIdInBody.get()).isEqualTo(methodSpan.getSpanId());
               });
    }

    private AxonConfiguration startProbeApplication(AsyncInMemoryStreamableEventSource eventSource,
                                                    CurrentSpanProbeProjection projection) {
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> registry
                                          // Stay local: no Axon Server connector.
                                          .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                          .registerComponent(SpanFactory.class, c -> spanFactory)
                                          .registerComponent(MessagingTracingSettings.class,
                                                             c -> MessagingTracingSettings.enabledByDefault())
                                          .registerComponent(Tracer.class, c -> tracing.tracer())
                                  )
                                  .eventProcessing(ep -> ep.pooledStreaming(
                                          ps -> ps.defaults(d -> d.eventSource(eventSource))
                                                  .defaultProcessor(
                                                          "psep-tracing-test-current-span-probe-processor",
                                                          c -> c.autodetected("current-span-probe",
                                                                              cfg -> projection)
                                                  )))
                                  .start();
    }

    /**
     * Guards the uncached {@code CommandDispatcher#forContext}: with the (former) per-context caching, the first
     * event's parameter resolution stored a dispatcher capturing the FIRST event's context branch on the shared batch
     * context, so the second event's dispatch resolved its parent from event 1's branch and mis-parented under event
     * 1's spans. Both handlers here take the injected {@link CommandDispatcher} and dispatch the same command type,
     * so the two dispatch spans must parent under their own events' method spans -- one each.
     */
    @Test
    void bothEventsDispatchingThroughTheInjectedCommandDispatcherParentUnderTheirOwnEventSpans() {
        // given a streaming batch of two events whose handlers BOTH dispatch through the injected CommandDispatcher
        // (same single-batch setup as the mixed-batch regression test above)
        mixedBatchWorkerExecutor = delayedWorkerExecutor();
        AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();
        DualDispatchProjection projection = new DualDispatchProjection();
        eventSource.publishMessage(EventTestUtils.asEventMessage(new BookingRequested("room-9")));
        eventSource.publishMessage(EventTestUtils.asEventMessage(new BookingConfirmed("room-9")));

        configuration = startMixedBatchApplication(eventSource, projection,
                                                   settings(/* disableBatchTrace */ false,
                                                            /* distributedInSameTrace */ false));

        // when both events are handled, in one batch, each dispatching a command
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(projection.firstHandled.get()).isEqualTo("room-9"));
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(projection.secondHandled.get()).isEqualTo("room-9"));

        String methodSpan1Name = "DualDispatchProjection.on(BookingRequested,CommandDispatcher)";
        String methodSpan2Name = "DualDispatchProjection.on(BookingConfirmed,CommandDispatcher)";

        // then each dispatch span parents under its OWN event's method span -- one each, not both under event 1's
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> {
                   SpanData methodSpan1 = spanNamed(methodSpan1Name);
                   SpanData methodSpan2 = spanNamed(methodSpan2Name);
                   List<String> dispatchParents = spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().startsWith("CommandBus.dispatchCommand"))
                           .map(span -> span.getParentSpanContext().getSpanId())
                           .toList();
                   assertThat(dispatchParents)
                           .containsExactlyInAnyOrder(methodSpan1.getSpanId(), methodSpan2.getSpanId());
               });
    }

    /**
     * Annotated event handler for the dual-dispatch regression test: BOTH events dispatch through the injected
     * {@link CommandDispatcher}, so a per-batch-cached dispatcher would attach the second dispatch to the first
     * event's context branch.
     */
    @SuppressWarnings("unused")
    static class DualDispatchProjection {

        private final AtomicReference<String> firstHandled = new AtomicReference<>();
        private final AtomicReference<String> secondHandled = new AtomicReference<>();

        @EventHandler
        public void on(BookingRequested event, CommandDispatcher commandDispatcher) {
            firstHandled.set(event.roomId());
            commandDispatcher.send(new ConfirmBooking(event.roomId()));
        }

        @EventHandler
        public void on(BookingConfirmed event, CommandDispatcher commandDispatcher) {
            secondHandled.set(event.roomId());
            commandDispatcher.send(new ConfirmBooking(event.roomId()));
        }
    }

    static class CurrentSpanProbeProjection {

        private final Tracer tracer;
        private final AtomicReference<@Nullable String> currentSpanIdInBody = new AtomicReference<>();
        private final java.util.concurrent.atomic.AtomicBoolean handled =
                new java.util.concurrent.atomic.AtomicBoolean(false);

        CurrentSpanProbeProjection(Tracer tracer) {
            this.tracer = tracer;
        }

        @EventHandler
        public void on(String event) {
            io.micrometer.tracing.Span current = tracer.currentSpan();
            currentSpanIdInBody.set(current == null ? null : current.context().spanId());
            handled.set(true);
        }
    }

    private static MessagingTracingSettings settings(boolean disableBatchTrace, boolean distributedInSameTrace) {
        return new MessagingTracingSettings(
                true, true, true,
                disableBatchTrace, distributedInSameTrace,
                MessagingTracingSettings.DEFAULT_DISTRIBUTED_IN_SAME_TRACE_TIME_LIMIT,
                true, false, MessagingTracingSettings.SpanAttributesProviders.enabledByDefault());
    }

    private AxonConfiguration startApplication(AsyncInMemoryStreamableEventSource eventSource,
                                               BookingProjection projection,
                                               MessagingTracingSettings settings) {
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> registry
                                          // Stay local: no Axon Server connector.
                                          .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                          .registerComponent(SpanFactory.class, c -> spanFactory)
                                          .registerComponent(MessagingTracingSettings.class, c -> settings)
                                          // Required by the auto-discovered thread-local context-propagation
                                          // enhancer (io.axoniq.framework.tracing.micrometer.threadlocal), which
                                          // reads the Tracer component regardless of this test's own SpanFactory
                                          // wiring.
                                          .registerComponent(Tracer.class, c -> tracing.tracer())
                                  )
                                  .eventProcessing(ep -> ep.pooledStreaming(
                                          ps -> ps.defaults(d -> d.eventSource(eventSource))
                                                  .defaultProcessor(
                                                          "psep-tracing-test-processor",
                                                          c -> c.autodetected("bookings", cfg -> projection)
                                                  )
                                  ))
                                  .start();
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

    /**
     * Annotated event handler recording the payload it processed.
     */
    @SuppressWarnings("unused")
    static class BookingProjection {

        private final AtomicReference<String> handledEvent = new AtomicReference<>();

        @EventHandler
        public void on(String event) {
            handledEvent.set(event);
        }
    }

    private record BookingRequested(String roomId) {

    }

    private record BookingConfirmed(String roomId) {

    }

    private record ConfirmBooking(String roomId) {

    }

    private record AuditLogged(String roomId) {

    }

    @SuppressWarnings("unused")
    static class ConfirmBookingHandler {

        @CommandHandler
        public void handle(ConfirmBooking command) {
            // no-op: only dispatch-side tracing is under test here
        }
    }

    /**
     * Annotated event handler for the two-event single-batch regression test: the first event dispatches a command,
     * the second appends a follow-up event.
     */
    @SuppressWarnings("unused")
    static class MixedBatchProjection {

        private final AtomicReference<String> firstHandled = new AtomicReference<>();
        private final AtomicReference<String> secondHandled = new AtomicReference<>();

        @EventHandler
        public void on(BookingRequested event, CommandDispatcher commandDispatcher) {
            firstHandled.set(event.roomId());
            commandDispatcher.send(new ConfirmBooking(event.roomId()));
        }

        @EventHandler
        public void on(BookingConfirmed event, EventAppender eventAppender) {
            secondHandled.set(event.roomId());
            eventAppender.append(new AuditLogged(event.roomId()));
        }
    }
}
