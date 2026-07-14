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

package io.axoniq.framework.tracing.micrometer;

import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.tracing.Span;
import org.axonframework.messaging.tracing.SpanScope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lifecycle-level tests for {@link MicrometerSpan} against Micrometer's {@link SimpleTracer} recording double: lazy
 * start, double-start guarding, attribute routing before/after start, error mapping, {@link Span#propagateContext}
 * no-op before start, current-span resource restore, and structured scope execution making the span current.
 */
class MicrometerSpanLifecycleTest {

    private SimpleTracer tracer;
    private MicrometerSpanFactory factory;

    @BeforeEach
    void setUp() {
        tracer = new SimpleTracer();
        factory = new MicrometerSpanFactory(tracer, Propagator.NOOP);
    }

    private static Message anEvent() {
        return EventTestUtils.asEventMessage("MyPayload");
    }

    @Nested
    class LazyStart {

        @Test
        void underlyingSpanIsNotCreatedUntilStart() {
            // given
            factory.createInternalSpan("Lazy", null);

            // then — no span was materialized on the tracer
            assertThat(tracer.getSpans()).isEmpty();
        }

        @Test
        void startMaterializesTheSpan() {
            // given
            Span span = factory.createInternalSpan("Started", null);

            // when
            span.start().close();

            // then
            assertThat(tracer.getSpans()).hasSize(1);
            assertThat(tracer.getSpans().getFirst().getName()).isEqualTo("Started");
        }

        @Test
        void secondStartDoesNotMaterializeASecondSpan() {
            // given
            Span span = factory.createInternalSpan("Once", null);

            // when started twice (the second start is guarded and only logs a warning)
            SpanScope first = span.start();
            SpanScope second = span.start();

            // then
            assertThat(tracer.getSpans()).hasSize(1);
            second.close();
            first.close();
        }

        @Test
        void repeatedStartHandlesShareTheUnderlyingSpansClosedState() {
            Span span = factory.createInternalSpan("Once", null);
            SpanScope first = span.start();
            SpanScope second = span.start();

            assertThat(second).isSameAs(first);
            assertThat(first.isClosed()).isFalse();
            assertThat(second.isClosed()).isFalse();
            first.close();
            assertThat(first.isClosed()).isTrue();
            assertThat(second.isClosed()).isTrue();
        }

        @Test
        void concurrentStartsMaterializeAndShareOneScope() throws Exception {
            Span span = factory.createInternalSpan("Once", null);
            int callers = 4;
            CyclicBarrier startGate = new CyclicBarrier(callers);

            try (ExecutorService executor = Executors.newFixedThreadPool(callers)) {
                List<Future<SpanScope>> futures = IntStream.range(0, callers)
                                                            .mapToObj(ignored -> executor.submit(() -> {
                                                                startGate.await();
                                                                return span.start();
                                                            }))
                                                            .toList();
                List<SpanScope> scopes = futures.stream().map(future -> {
                    try {
                        return future.get(2, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).toList();

                assertThat(tracer.getSpans()).hasSize(1);
                assertThat(scopes).allSatisfy(scope -> assertThat(scope).isSameAs(scopes.getFirst()));

                scopes.getFirst().close();
                assertThat(scopes).allSatisfy(scope -> assertThat(scope.isClosed()).isTrue());
            }
        }
    }

    @Nested
    class Attributes {

        @Test
        void attributeAddedBeforeStartIsAppliedToTheSpan() {
            // given
            Span span = factory.createInternalSpan("Tagged", null);
            span.addAttribute("key.before", "value-before");

            // when
            span.start().close();

            // then
            assertThat(tracer.getSpans().getFirst().getTags()).containsEntry("key.before", "value-before");
        }

        @Test
        void attributeAddedAfterStartIsAppliedToTheSpan() {
            // given
            Span span = factory.createInternalSpan("Tagged", null);

            // when
            SpanScope scope = span.start();
            span.addAttribute("key.after", "value-after");
            scope.close();

            // then
            assertThat(tracer.getSpans().getFirst().getTags()).containsEntry("key.after", "value-after");
        }
    }

    @Nested
    class ErrorRecording {

        @Test
        void recordExceptionAfterStartMapsToSpanError() {
            // given
            Span span = factory.createInternalSpan("Erroring", null);
            RuntimeException failure = new RuntimeException("boom");

            // when
            SpanScope scope = span.start();
            span.recordException(failure);
            scope.close();

            // then
            assertThat(tracer.getSpans().getFirst().getError()).isSameAs(failure);
        }

        @Test
        void recordExceptionBeforeStartIsANoOp() {
            // given
            Span span = factory.createInternalSpan("NotStarted", null);

            // when — no span exists yet; the call is a guarded no-op
            span.recordException(new RuntimeException("ignored"));

            // then
            assertThat(tracer.getSpans()).isEmpty();
        }
    }

    @Nested
    class Propagation {

        @Test
        void propagateContextBeforeStartReturnsTheMessageUnchanged() {
            // given
            Span span = factory.createInternalSpan("NotStarted", null);
            Message message = anEvent();

            // when
            Message result = span.propagateContext(message);

            // then
            assertThat(result).isSameAs(message);
        }
    }

    /**
     * Single-key branch model: {@link MicrometerSpan} carries no {@link ProcessingContext} of its own and writes no
     * resource on {@link #start()} -- nesting is entirely the caller's concern, expressed through
     * {@link SpanScope#RESOURCE_KEY}, either on the root ({@link Span#coverLifecycle(ProcessingContext)}) or on an
     * immutable branch ({@link SpanScope#addToContext}, branch-scoped). This replaces the former
     * push/pop-stack contract this class used to test.
     */
    @Nested
    class SingleKeyBranching {

        @Test
        void imperativeStartWritesNoContextResource() {
            // given
            StubProcessingContext context = new StubProcessingContext();
            Span span = factory.createInternalSpan("Scoped", context);

            // when
            SpanScope scope = span.start();

            // then -- the imperative start() never touches the context; only addToContext / coverLifecycle do
            assertThat(SpanScope.fromContext(context)).isNull();
            scope.close();
        }

        @Test
        void addingTheScopeToABranchIsolatesItFromTheOriginalContext() {
            // given a real (branching) ProcessingContext -- unlike StubProcessingContext, which mutates its
            // resources map in place instead of branching on withResource, masking exactly the isolation this test
            // guards
            UnitOfWork unitOfWork = UnitOfWorkTestUtils.aUnitOfWork();
            unitOfWork.executeWithResult(context -> {
                Span span = factory.createInternalSpan("Scoped", context);
                SpanScope scope = span.start();

                // when the scope is carried on a branch, not written to the original context
                ProcessingContext branch = SpanScope.addToContext(context, scope);

                // then the branch resolves the scope for its own children, while the original context stays
                // untouched -- even after the branch is created, a sibling created directly against the original
                // context would still be a root
                assertThat(SpanScope.fromContext(branch)).isSameAs(scope);
                assertThat(SpanScope.fromContext(context)).isNull();
                scope.close();
                return CompletableFuture.completedFuture(null);
            }).join();
        }

        @Test
        void closeIsIdempotent() {
            // given
            Span span = factory.createInternalSpan("Once", null);
            SpanScope scope = span.start();

            // when closed twice
            scope.close();
            scope.close();

            // then the underlying span still just ended (no exception, no double-end error)
            assertThat(tracer.getSpans().getFirst().getEndTimestamp()).isAfter(Instant.EPOCH);
        }

        @Test
        void withinMakesTheSpanCurrentOnlyForTheOperationsDurationAndReturnsItsValue() {
            // given
            Span span = factory.createInternalSpan("Current", null);
            SpanScope scope = span.start();

            // when
            AtomicReference<String> currentSpanIdInside = new AtomicReference<>();
            String result = scope.within(() -> {
                currentSpanIdInside.set(tracer.currentSpan().context().spanId());
                return "result";
            });

            // then
            assertThat(result).isEqualTo("result");
            assertThat(currentSpanIdInside.get()).isEqualTo(tracer.getSpans().getFirst().context().spanId());
            assertThat(tracer.currentSpan()).isNull();
            scope.close();
        }

        @Test
        void withinPropagatesTheOperationsExceptionAndRestoresThePreviousCurrentSpan() {
            // given an outer span that is the tracer's current span before the operation
            io.micrometer.tracing.Span outer = tracer.nextSpan().name("Outer").start();
            Span span = factory.createInternalSpan("Current", null);
            SpanScope scope = span.start();
            RuntimeException failure = new RuntimeException("boom");

            try (io.micrometer.tracing.Tracer.SpanInScope ignored = tracer.withSpan(outer)) {
                // when / then
                assertThatThrownBy(() -> scope.within(() -> {
                    assertThat(tracer.currentSpan()).isNotNull();
                    throw failure;
                })).isSameAs(failure);

                // then the previous current span is restored, not merely cleared
                assertThat(tracer.currentSpan().context().spanId()).isEqualTo(outer.context().spanId());
            }
            scope.close();
        }

        @Test
        void withinRestoresThePreviousCurrentSpanWhenNestedInsideAnotherScopeWindow() {
            // given an outer span current on the thread -- as around every pull of a scoped stream that executes
            // inside an enclosing operation's window
            io.micrometer.tracing.Span outer = tracer.nextSpan().name("Outer").start();
            Span inner = factory.createInternalSpan("Inner", null);
            SpanScope innerScope = inner.start();

            try (io.micrometer.tracing.Tracer.SpanInScope ignored = tracer.withSpan(outer)) {
                // when the inner scope's window executes
                String innerIdInside = innerScope.within(() -> tracer.currentSpan().context().spanId());

                // then the inner span was current inside the window and the outer span is current again afterwards
                assertThat(innerIdInside).isNotEqualTo(outer.context().spanId());
                assertThat(tracer.currentSpan().context().spanId()).isEqualTo(outer.context().spanId());
            }
            innerScope.close();
        }
    }

    @Nested
    class StructuredBranchOperations {

        @Test
        void branchMakesTheSpanCurrentAndRestoresAfterwards() {
            // given
            Span span = factory.createInternalSpan("Traced", null);
            AtomicReference<String> currentSpanIdInside = new AtomicReference<>();

            // when
            String result = span.branch(null, ignored -> {
                currentSpanIdInside.set(tracer.currentSpan().context().spanId());
                return "result";
            });

            // then — the span was current inside the block and cleared afterwards
            assertThat(result).isEqualTo("result");
            SimpleSpan ended = tracer.getSpans().getLast();
            assertThat(currentSpanIdInside.get()).isEqualTo(ended.context().spanId());
            assertThat(tracer.currentSpan()).isNull();
            assertThat(ended.getEndTimestamp()).isAfter(Instant.EPOCH);
        }

        @Test
        void branchRecordsExceptionAndRethrows() {
            // given
            Span span = factory.createInternalSpan("Failing", null);
            RuntimeException failure = new RuntimeException("boom");

            // when / then
            assertThatThrownBy(() -> span.branch(null, ignored -> {
                throw failure;
            })).isSameAs(failure);
            assertThat(tracer.getSpans().getFirst().getError()).isSameAs(failure);
        }

        @Test
        void branchAsyncEndsTheSpanWhenTheFutureCompletes() {
            // given
            Span span = factory.createInternalSpan("Async", null);
            CompletableFuture<String> gate = new CompletableFuture<>();

            // when
            CompletableFuture<String> result = span.branchAsync(null, ignored -> gate);

            // then — span not ended until the future completes (SimpleSpan reports Instant.EPOCH while open)
            assertThat(tracer.getSpans().getFirst().getEndTimestamp()).isEqualTo(Instant.EPOCH);
            gate.complete("done");
            assertThat(result.orTimeout(5, TimeUnit.SECONDS).join()).isEqualTo("done");
            assertThat(tracer.getSpans().getFirst().getEndTimestamp()).isAfter(Instant.EPOCH);
        }

        @Test
        void branchAsyncRecordsExceptionWhenTheFutureFails() {
            // given
            Span span = factory.createInternalSpan("AsyncFailing", null);
            RuntimeException failure = new RuntimeException("async-boom");

            // when
            CompletableFuture<String> result =
                    span.branchAsync(null, ignored -> CompletableFuture.failedFuture(failure));

            // then
            assertThatThrownBy(() -> result.orTimeout(5, TimeUnit.SECONDS).join()).hasRootCause(failure);
            assertThat(tracer.getSpans().getFirst().getError()).isSameAs(failure);
        }
    }
}
