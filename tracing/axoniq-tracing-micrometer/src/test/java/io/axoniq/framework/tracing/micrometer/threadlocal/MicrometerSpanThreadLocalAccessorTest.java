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


package io.axoniq.framework.tracing.micrometer.threadlocal;

import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ContextSnapshot;
import io.micrometer.context.ContextSnapshotFactory;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.contextpropagation.ObservationAwareSpanThreadLocalAccessor;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.*;

/**
 * Tests {@link MicrometerSpanThreadLocalAccessor}: every set and clear of the propagated span opens its own scope and
 * every restore closes exactly one, so nested context snapshots unwind to the span that was current before them.
 *
 * @author Mateusz Nowak
 */
class MicrometerSpanThreadLocalAccessorTest {

    private SdkTracerProvider tracerProvider;
    private Tracer tracer;
    private MicrometerSpanThreadLocalAccessor testSubject;

    @BeforeEach
    void setUp() {
        tracerProvider = SdkTracerProvider.builder()
                                          .addSpanProcessor(SimpleSpanProcessor.create(InMemorySpanExporter.create()))
                                          .build();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                                                         .setTracerProvider(tracerProvider)
                                                         .build();
        tracer = new OtelTracer(openTelemetry.getTracer("AxoniqFramework"),
                                new OtelCurrentTraceContext(),
                                event -> {
                                });
        testSubject = new MicrometerSpanThreadLocalAccessor(ObservationRegistry.create(), tracer);
    }

    @AfterEach
    void tearDown() {
        tracerProvider.close();
    }

    @Nested
    class NestedScopes {

        @Test
        void restoresEveryNestedSpanAndClearScopeInLifoOrder() {
            // given
            Span parent = tracer.nextSpan().name("parent").start();

            // when / then every set and clear opens its own scope...
            testSubject.setValue(parent);
            assertThat(tracer.currentSpan()).isEqualTo(parent);
            testSubject.setValue(parent);
            assertThat(tracer.currentSpan()).isEqualTo(parent);
            testSubject.setValue();
            assertThat(tracer.currentSpan()).isNull();
            testSubject.setValue(parent);
            assertThat(tracer.currentSpan()).isEqualTo(parent);

            // ...and every restore closes exactly one of them, innermost first
            testSubject.restore();
            assertThat(tracer.currentSpan()).isNull();
            testSubject.restore(parent);
            assertThat(tracer.currentSpan()).isEqualTo(parent);
            testSubject.restore(parent);
            assertThat(tracer.currentSpan()).isEqualTo(parent);
            testSubject.restore();
            assertThat(tracer.currentSpan()).isNull();
        }

        @Test
        void nestedSnapshotWithoutSpanRestoresTheOuterSpanWhenItsScopeCloses() {
            // given
            ContextSnapshotFactory snapshotFactory =
                    ContextSnapshotFactory.builder()
                                          .contextRegistry(new ContextRegistry().loadContextAccessors()
                                                                                .registerThreadLocalAccessor(
                                                                                        testSubject))
                                          .clearMissing(true)
                                          .build();
            Span parent = tracer.nextSpan().name("parent").start();
            ContextSnapshot withSpan =
                    snapshotFactory.captureFrom(Context.of(ObservationAwareSpanThreadLocalAccessor.KEY, parent));
            ContextSnapshot withoutSpan = snapshotFactory.captureFrom(Context.empty());

            // when / then
            try (ContextSnapshot.Scope outer = withSpan.setThreadLocals()) {
                try (ContextSnapshot.Scope inner = withSpan.setThreadLocals()) {
                    try (ContextSnapshot.Scope cleared = withoutSpan.setThreadLocals()) {
                        assertThat(tracer.currentSpan()).isNull();
                    }
                    assertThat(tracer.currentSpan()).isEqualTo(parent);
                }
                assertThat(tracer.currentSpan()).isEqualTo(parent);
            } finally {
                parent.end();
            }
            assertThat(tracer.currentSpan()).isNull();
        }

        @Test
        void clearWithoutAnOwnedRawSpanScopeDoesNotHideTheCurrentHostSpan() {
            // given a span made current outside of this accessor, as Spring's HTTP Observation does
            Span hostSpan = tracer.nextSpan().name("host-observation").start();

            try (Tracer.SpanInScope ignored = tracer.withSpan(hostSpan)) {
                // when
                testSubject.setValue();

                // then
                assertThat(tracer.currentSpan()).isEqualTo(hostSpan);
                testSubject.restore();
                assertThat(tracer.currentSpan()).isEqualTo(hostSpan);
            } finally {
                hostSpan.end();
            }
            assertThat(tracer.currentSpan()).isNull();
        }
    }

    @Nested
    class ReactorAutomaticContextPropagation {

        @BeforeEach
        void enableAutomaticContextPropagation() {
            ContextRegistry.getInstance().registerThreadLocalAccessor(testSubject);
            Hooks.enableAutomaticContextPropagation();
        }

        @AfterEach
        void disableAutomaticContextPropagation() {
            Hooks.disableAutomaticContextPropagation();
            ContextRegistry.getInstance().removeThreadLocalAccessor(ObservationAwareSpanThreadLocalAccessor.KEY);
        }

        @Test
        void subscriberWithoutSpanDoesNotLeaveTheSpanCurrentOnTheWorkerThread() throws InterruptedException {
            // given
            ExecutorService worker = Executors.newSingleThreadExecutor();
            Scheduler scheduler = Schedulers.fromExecutorService(worker);
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            Hooks.onErrorDropped(errors::add);
            Span span = tracer.nextSpan().name("handler").start();
            try {
                CountDownLatch done = new CountDownLatch(1);
                AtomicReference<Span> spanInOperator = new AtomicReference<>();

                // when the subscriber's Reactor Context has no span, the span is cleared while it is still in scope
                Mono.just("event")
                    .publishOn(scheduler)
                    .<String>handle((value, sink) -> {
                        spanInOperator.set(tracer.currentSpan());
                        sink.next(value);
                    })
                    .contextWrite(Context.of(ObservationAwareSpanThreadLocalAccessor.KEY, span))
                    .doFinally(signal -> done.countDown())
                    .subscribe(value -> {
                    }, errors::add);

                // then the operator saw the span, and a raw task on the same worker no longer does
                assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(errors).isEmpty();
                assertThat(spanInOperator.get()).isEqualTo(span);
                assertThat(worker.submit(() -> tracer.currentSpan())).succeedsWithin(5, TimeUnit.SECONDS)
                                                                     .isNull();
            } finally {
                span.end();
                Hooks.resetOnErrorDropped();
                scheduler.dispose();
            }
        }
    }

    @Nested
    class FailureContainment {

        @Test
        void tracingScopeCloseFailureDoesNotEscapeIntoApplicationProcessing() {
            // given a tracer whose scopes restore the previous span and then fail on close
            Tracer failingTracer = spy(tracer);
            doAnswer(invocation -> {
                Tracer.SpanInScope scope = (Tracer.SpanInScope) invocation.callRealMethod();
                return (Tracer.SpanInScope) () -> {
                    scope.close();
                    throw new IllegalStateException("broken tracing scope");
                };
            }).when(failingTracer).withSpan(any());
            MicrometerSpanThreadLocalAccessor failingAccessor =
                    new MicrometerSpanThreadLocalAccessor(ObservationRegistry.create(), failingTracer);
            Span parent = tracer.nextSpan().name("parent").start();
            failingAccessor.setValue(parent);

            // when / then
            assertThatCode(failingAccessor::restore).doesNotThrowAnyException();
            assertThat(tracer.currentSpan()).isNull();
            parent.end();
        }

        @Test
        void tracingScopeOpenFailureDoesNotEscapeIntoApplicationProcessing() {
            // given a tracer that fails to open scopes
            Tracer failingTracer = spy(tracer);
            doThrow(new IllegalStateException("broken tracing backend")).when(failingTracer).withSpan(any());
            MicrometerSpanThreadLocalAccessor failingAccessor =
                    new MicrometerSpanThreadLocalAccessor(ObservationRegistry.create(), failingTracer);
            Span parent = tracer.nextSpan().name("parent").start();

            // when / then the failed open still pairs with its restore
            assertThatCode(() -> {
                failingAccessor.setValue(parent);
                failingAccessor.restore();
            }).doesNotThrowAnyException();
            assertThat(tracer.currentSpan()).isNull();
            parent.end();
        }
    }
}
