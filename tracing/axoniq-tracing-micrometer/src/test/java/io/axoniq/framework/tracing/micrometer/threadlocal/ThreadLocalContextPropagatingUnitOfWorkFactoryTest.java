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

import io.axoniq.framework.tracing.micrometer.MicrometerSpanFactory;
import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ContextSnapshotFactory;
import io.micrometer.context.ThreadLocalAccessor;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle.DefaultPhases;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.tracing.Span;
import org.axonframework.messaging.tracing.SpanScope;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.axonframework.common.FutureUtils.emptyCompletedFuture;

/**
 * Tests {@link ThreadLocalContextPropagatingUnitOfWorkFactory}: caller thread-locals captured at creation are restored inside
 * phase actions that run on worker threads, the per-action Axon span (on the {@link
 * org.axonframework.messaging.core.unitofwork.ProcessingContext}) is made current, and thread-locals are torn down
 * cleanly after completion and on exception.
 */
class ThreadLocalContextPropagatingUnitOfWorkFactoryTest {

    private static final ThreadLocal<String> CORRELATION = new ThreadLocal<>();

    private SdkTracerProvider tracerProvider;
    private Tracer tracer;
    private MicrometerSpanFactory spanFactory;
    private ExecutorService worker;
    private ThreadLocalContextPropagatingUnitOfWorkFactory testSubject;

    @BeforeEach
    void setUp() {
        InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                                          .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                                          .build();
        ContextPropagators contextPropagators = ContextPropagators.create(W3CTraceContextPropagator.getInstance());
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                                                         .setTracerProvider(tracerProvider)
                                                         .setPropagators(contextPropagators)
                                                         .build();
        io.opentelemetry.api.trace.Tracer otelTracer = openTelemetry.getTracer("AxoniqFramework");
        tracer = new OtelTracer(otelTracer, new OtelCurrentTraceContext(), event -> {
        });
        Propagator propagator = new OtelPropagator(contextPropagators, otelTracer);
        spanFactory = new MicrometerSpanFactory(tracer, propagator);

        worker = Executors.newSingleThreadExecutor(r -> new Thread(r, "uow-worker"));
        // Isolated registry so the test's ThreadLocal accessor does not pollute the global ContextRegistry.
        ContextRegistry contextRegistry = new ContextRegistry().registerThreadLocalAccessor(new CorrelationAccessor());
        ContextSnapshotFactory snapshotFactory = ContextSnapshotFactory.builder()
                                                                       .contextRegistry(contextRegistry)
                                                                       .build();
        UnitOfWorkFactory delegate = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);
        testSubject = new ThreadLocalContextPropagatingUnitOfWorkFactory(delegate, tracer, snapshotFactory);
    }

    @AfterEach
    void tearDown() {
        worker.shutdownNow();
        tracerProvider.close();
        CORRELATION.remove();
    }

    @Nested
    class ThreadLocalRestoration {

        @Test
        void restoresCallerThreadLocalsInsidePhaseActionsOnWorkerThread() {
            // given the caller thread carries a correlation value when the unit of work is created
            CORRELATION.set("req-1");
            UnitOfWork unitOfWork = testSubject.create("uow", config -> config.workScheduler(worker));
            // clear it on the caller to prove the value seen inside comes from the captured snapshot, not ambient
            CORRELATION.remove();

            AtomicReference<String> seenValue = new AtomicReference<>();
            AtomicReference<String> actionThread = new AtomicReference<>();
            unitOfWork.on(DefaultPhases.INVOCATION, pc -> {
                actionThread.set(Thread.currentThread().getName());
                seenValue.set(CORRELATION.get());
                return emptyCompletedFuture();
            });

            // when
            unitOfWork.execute().join();

            // then the action ran on the worker thread yet saw the caller's correlation value
            assertThat(actionThread.get()).isEqualTo("uow-worker");
            assertThat(seenValue.get()).isEqualTo("req-1");
        }

        @Test
        void workerThreadLocalsAreCleanAfterCompletion() throws Exception {
            // given
            CORRELATION.set("req-2");
            UnitOfWork unitOfWork = testSubject.create("uow", config -> config.workScheduler(worker));
            CORRELATION.remove();
            unitOfWork.on(DefaultPhases.INVOCATION, pc -> emptyCompletedFuture());

            // when
            unitOfWork.execute().join();

            // then the worker thread has no leaked correlation value
            assertThat(readCorrelationOnWorker()).isNull();
        }

        @Test
        void workerThreadLocalsAreCleanEvenWhenAnActionThrows() throws Exception {
            // given
            CORRELATION.set("req-3");
            UnitOfWork unitOfWork = testSubject.create("uow", config -> config.workScheduler(worker));
            CORRELATION.remove();
            unitOfWork.on(DefaultPhases.INVOCATION, pc -> CompletableFuture.failedFuture(new RuntimeException("boom")));

            // when / then — failure propagates and the worker thread-local is still cleaned up
            assertThatThrownBy(() -> unitOfWork.execute().join()).hasRootCauseMessage("boom");
            assertThat(readCorrelationOnWorker()).isNull();
        }

        private @Nullable String readCorrelationOnWorker() throws Exception {
            return worker.submit(CORRELATION::get).get();
        }
    }

    @Nested
    class PerActionCurrentSpan {

        @Test
        void perActionSpanOnTheProcessingContextIsMadeCurrent() {
            // given a segment span placed on the ProcessingContext in an earlier phase
            UnitOfWork unitOfWork = testSubject.create("uow", config -> config.workScheduler(worker));
            AtomicReference<String> segmentSpanId = new AtomicReference<>();
            AtomicReference<String> currentSpanIdInInvocation = new AtomicReference<>();

            unitOfWork.on(DefaultPhases.PRE_INVOCATION, pc -> {
                // Lifecycle-covering bind: this span must still be resolvable, unchanged, from the root
                // context in the later INVOCATION phase action, which is a *different* phase action entirely.
                Span segment = spanFactory.createInternalSpan("segment", pc);
                segment.coverLifecycle(pc);
                io.micrometer.tracing.Span current = MicrometerSpanFactory.rawSpanFrom(pc);
                segmentSpanId.set(current.context().spanId());
                return emptyCompletedFuture();
            });
            unitOfWork.on(DefaultPhases.INVOCATION, pc -> {
                currentSpanIdInInvocation.set(tracer.currentSpan().context().spanId());
                return emptyCompletedFuture();
            });

            // when
            unitOfWork.execute().join();

            // then the INVOCATION action ran with the segment span current (its own segment, not a caller span)
            assertThat(currentSpanIdInInvocation.get()).isEqualTo(segmentSpanId.get());
        }

        @Test
        void childSpanCreatedDuringAnActionParentsOnTheProcessingContextResource() {
            // given a segment span current on the ProcessingContext
            UnitOfWork unitOfWork = testSubject.create("uow", config -> config.workScheduler(worker));
            AtomicReference<String> segmentSpanId = new AtomicReference<>();
            AtomicReference<String> childParentSpanId = new AtomicReference<>();

            unitOfWork.on(DefaultPhases.PRE_INVOCATION, pc -> {
                Span segment = spanFactory.createInternalSpan("segment", pc);
                segment.coverLifecycle(pc);
                segmentSpanId.set(MicrometerSpanFactory.rawSpanFrom(pc).context().spanId());
                return emptyCompletedFuture();
            });
            unitOfWork.on(DefaultPhases.INVOCATION, pc -> {
                // a child dispatch created with the same context must parent on the resource span, not the ambient one
                Span child = spanFactory.createDispatchSpan("child", EventTestUtils.asEventMessage("p"), pc);
                SpanScope childScope = child.start();
                ProcessingContext branch = SpanScope.addToContext(pc, childScope);
                io.micrometer.tracing.Span childSpan = MicrometerSpanFactory.rawSpanFrom(branch);
                childParentSpanId.set(childSpan.context().parentId());
                return emptyCompletedFuture();
            });

            // when
            unitOfWork.execute().join();

            // then the child's parent is the segment span from the ProcessingContext resource
            assertThat(childParentSpanId.get()).isEqualTo(segmentSpanId.get());
        }
    }

    private static final class CorrelationAccessor implements ThreadLocalAccessor<String> {

        @Override
        public Object key() {
            return "test.correlation";
        }

        @Override
        public @Nullable String getValue() {
            return CORRELATION.get();
        }

        @Override
        public void setValue(String value) {
            CORRELATION.set(value);
        }

        @Override
        public void setValue() {
            CORRELATION.remove();
        }
    }
}
