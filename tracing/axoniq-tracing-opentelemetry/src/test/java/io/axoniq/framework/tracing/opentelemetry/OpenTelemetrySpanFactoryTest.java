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

package io.axoniq.framework.tracing.opentelemetry;

import io.axoniq.framework.tracing.Span;
import io.axoniq.framework.tracing.SpanScope;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests {@link OpenTelemetrySpanFactory} against a real OpenTelemetry SDK using an in-memory span exporter as the
 * recording double, asserting on the exported {@link SpanData}.
 * <p>
 * The tests deliberately never touch OpenTelemetry's thread-local {@code Context.current()} / {@code makeCurrent()}.
 * Cross-boundary parenting rides on message metadata (W3C trace context) and in-process nesting rides on the
 * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} resource the factory and spans share.
 */
class OpenTelemetrySpanFactoryTest {

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private OpenTelemetry openTelemetry;
    private OpenTelemetrySpanFactory factory;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                                          .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                                          .build();
        TextMapPropagator propagator = W3CTraceContextPropagator.getInstance();
        openTelemetry = OpenTelemetrySdk.builder()
                                        .setTracerProvider(tracerProvider)
                                        .setPropagators(ContextPropagators.create(propagator))
                                        .build();
        factory = new OpenTelemetrySpanFactory(openTelemetry);
    }

    @AfterEach
    void tearDown() {
        tracerProvider.close();
    }

    private static Message anEvent() {
        return EventTestUtils.asEventMessage("MyPayload");
    }

    private SpanData exportedSpan() {
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(1);
        return spans.get(0);
    }

    private SpanData exportedSpanNamed(String name) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().equals(name))
                           .findFirst()
                           .orElseThrow();
    }

    @Nested
    class DispatchSpans {

        @Test
        void createDispatchSpanExportsProducerSpanWithName() {
            // given
            Span span = factory.createDispatchSpan("MyDispatch", anEvent(), null);

            // when
            try (SpanScope ignored = span.start()) {
                // no-op body
            }

            // then
            SpanData exported = exportedSpan();
            assertThat(exported.getName()).isEqualTo("MyDispatch");
            assertThat(exported.getKind()).isEqualTo(SpanKind.PRODUCER);
        }

        @Test
        void createDispatchSpanAppliesMessageAttributesFromRegisteredProvider() {
            // given
            Message message = anEvent();
            // inline SpanAttributesProvider lambda contributing a message attribute
            factory.registerAttributesProvider((m, c) -> Map.of("axoniq.message.id", m.identifier()));
            Span span = factory.createDispatchSpan("MyDispatch", message, null);

            // when
            try (SpanScope ignored = span.start()) {
                // no-op body
            }

            // then
            SpanData exported = exportedSpan();
            assertThat(exported.getAttributes().get(AttributeKey.stringKey("axoniq.message.id")))
                    .isEqualTo(message.identifier());
        }
    }

    @Nested
    class InternalSpans {

        @Test
        void createInternalSpanExportsInternalKindSpan() {
            // given
            Span span = factory.createInternalSpan("MyInternal", null);

            // when
            try (SpanScope ignored = span.start()) {
                // no-op body
            }

            // then
            SpanData exported = exportedSpan();
            assertThat(exported.getName()).isEqualTo("MyInternal");
            assertThat(exported.getKind()).isEqualTo(SpanKind.INTERNAL);
        }

        @Test
        void createRootSpanIsRootEvenWhenContextHasActiveSpan() {
            // given
            StubProcessingContext context = new StubProcessingContext();
            // an active span is recorded on the context by starting another span with it
            Span active = factory.createInternalSpan("Active", context);

            // when
            try (SpanScope ignored = active.start()) {
                Span root = factory.createRootSpan("MyRoot", context);
                try (SpanScope rootScope = root.start()) {
                    // no-op body
                }
            }

            // then
            SpanData rootSpanData = exportedSpanNamed("MyRoot");
            assertThat(rootSpanData.getKind()).isEqualTo(SpanKind.INTERNAL);
            // the root ignores the active span on the context: it has no parent
            assertThat(rootSpanData.getParentSpanContext().isValid()).isFalse();
        }
    }

    @Nested
    class CrossBoundaryPropagation {

        @Test
        void propagateContextWritesTraceparentMetadata() {
            // given
            Span dispatchSpan = factory.createDispatchSpan("MyDispatch", anEvent(), null);

            // when
            Message propagated;
            try (SpanScope ignored = dispatchSpan.start()) {
                propagated = dispatchSpan.propagateContext(anEvent());
            }

            // then
            assertThat(propagated.metadata()).containsKey("traceparent");
        }

        @Test
        void handlerSpanParentsOnSpanPropagatedThroughMessageMetadata() {
            // given a dispatch span that injects ITS OWN context into an outbound message, no thread-local involved
            Span dispatchSpan = factory.createDispatchSpan("MyDispatch", anEvent(), null);
            Message handledMessage;
            try (SpanScope ignored = dispatchSpan.start()) {
                handledMessage = dispatchSpan.propagateContext(anEvent());
            }
            SpanData dispatchSpanData = exportedSpanNamed("MyDispatch");

            // when the handler span is created from that message's metadata
            Span handlerSpan = factory.createHandlerSpan("MyHandler", handledMessage, null);
            try (SpanScope ignored = handlerSpan.start()) {
                // no-op body
            }

            // then it nests under the dispatch span, same trace, parent == dispatch span id
            SpanData handlerSpanData = exportedSpanNamed("MyHandler");
            assertThat(handlerSpanData.getKind()).isEqualTo(SpanKind.CONSUMER);
            assertThat(handlerSpanData.getTraceId()).isEqualTo(dispatchSpanData.getTraceId());
            assertThat(handlerSpanData.getParentSpanContext().getSpanId())
                    .isEqualTo(dispatchSpanData.getSpanId());
        }
    }

    @Nested
    class InProcessNestingViaProcessingContext {

        @Test
        void spanNestsUnderActiveSpanRecordedOnTheSameProcessingContext() {
            // given a shared ProcessingContext carrying the active span (no ThreadLocal, nothing made current)
            StubProcessingContext context = new StubProcessingContext();
            Span spanA = factory.createInternalSpan("A", context);

            // when B is created with the same context while A is active
            try (SpanScope scopeA = spanA.start()) {
                Span spanB = factory.createInternalSpan("B", context);
                try (SpanScope scopeB = spanB.start()) {
                    // no-op body; LIFO close order via try-with-resources
                }
            }

            // then B nests under A purely through the ProcessingContext resource
            SpanData spanAData = exportedSpanNamed("A");
            SpanData spanBData = exportedSpanNamed("B");
            assertThat(spanBData.getTraceId()).isEqualTo(spanAData.getTraceId());
            assertThat(spanBData.getParentSpanContext().getSpanId()).isEqualTo(spanAData.getSpanId());
        }

        @Test
        void afterActiveSpanScopeClosesNextSpanWithSameContextIsRootAgain() {
            // given
            StubProcessingContext context = new StubProcessingContext();
            Span spanA = factory.createInternalSpan("A", context);

            // when A's scope is closed before C is created with the same context
            try (SpanScope ignored = spanA.start()) {
                // A active here
            }
            Span spanC = factory.createInternalSpan("C", context);
            try (SpanScope ignored = spanC.start()) {
                // no-op body
            }

            // then C is a root again: the active span was cleared when A's scope closed
            SpanData spanCData = exportedSpanNamed("C");
            assertThat(spanCData.getParentSpanContext().isValid()).isFalse();
        }
    }

    @Nested
    class LinkedHandlerSpans {

        @Test
        void createLinkedHandlerSpanDoesNotThrowWithoutLinkableContext() {
            // given
            Message message = anEvent();
            Message linkedMessage = anEvent();

            // when / then
            assertThatCode(() -> {
                Span span = factory.createLinkedHandlerSpan("MyLinked", message, linkedMessage, null);
                try (SpanScope ignored = span.start()) {
                    // no-op body
                }
            }).doesNotThrowAnyException();

            SpanData exported = exportedSpan();
            assertThat(exported.getKind()).isEqualTo(SpanKind.CONSUMER);
        }

        @Test
        void createLinkedHandlerSpanAddsLinkToPropagatedSibling() {
            // given a sibling span that propagates its own context onto a message
            Span siblingSpan = factory.createDispatchSpan("Sibling", anEvent(), null);
            Message linkedMessage;
            try (SpanScope ignored = siblingSpan.start()) {
                linkedMessage = siblingSpan.propagateContext(anEvent());
            }
            SpanData siblingSpanData = exportedSpanNamed("Sibling");

            // when
            Span span = factory.createLinkedHandlerSpan("MyLinked", anEvent(), linkedMessage, null);
            try (SpanScope ignored = span.start()) {
                // no-op body
            }

            // then
            SpanData linkedSpanData = exportedSpanNamed("MyLinked");
            assertThat(linkedSpanData.getLinks()).hasSize(1);
            assertThat(linkedSpanData.getLinks().get(0).getSpanContext().getTraceId())
                    .isEqualTo(siblingSpanData.getTraceId());
        }
    }

    @Nested
    class MetadataContextPropagatorContract {

        @Test
        void fieldsReportReservedTraceparentKey() {
            // when / then
            assertThat(factory.fields()).contains("traceparent");
        }

        @Test
        void injectReturnsEmptyWhenContextHasNoActiveSpan() {
            // given
            StubProcessingContext context = new StubProcessingContext();

            // when
            Map<String, String> injected = factory.inject(context);

            // then
            assertThat(injected).isEmpty();
        }

        @Test
        void injectReturnsTraceparentAfterSpanStartedOnThatContext() {
            // given a span created with and started on the context (records it as the active span)
            StubProcessingContext context = new StubProcessingContext();
            Span span = factory.createInternalSpan("MyInternal", context);

            // when
            try (SpanScope ignored = span.start()) {
                Map<String, String> injected = factory.inject(context);

                // then
                assertThat(injected).containsKey("traceparent");
            }
        }
    }

    @Nested
    class CrossThreadNestingViaRealUnitOfWork {

        /**
         * The definitive no-{@code ThreadLocal} proof: a child span created and started on a <em>different thread</em>
         * than its parent still nests under the parent, purely because both resolve through the same
         * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} of a real {@link UnitOfWork}. A
         * thread-local-based implementation would make the child a root (the parent's thread-local is invisible on the
         * child thread), so this test would fail.
         */
        @Test
        void childSpanOnAnotherThreadNestsUnderParentThroughTheProcessingContext() {
            // given a real UnitOfWork; the parent span is started on the UoW thread
            UnitOfWork unitOfWork = UnitOfWorkTestUtils.aUnitOfWork();
            AtomicReference<Throwable> childThreadFailure = new AtomicReference<>();

            unitOfWork.executeWithResult(processingContext -> {
                Span parent = factory.createRootSpan("Parent", processingContext);
                SpanScope parentScope = parent.start();
                try {
                    // when the child span is created AND started on a different thread, sharing the same context
                    Thread childThread = new Thread(() -> {
                        try {
                            factory.createInternalSpan("Child", processingContext).start().close();
                        } catch (Throwable t) {
                            childThreadFailure.set(t);
                        }
                    });
                    childThread.start();
                    childThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                } finally {
                    parentScope.close();
                }
                return CompletableFuture.completedFuture(null);
            }).join();

            // then the child nests under the parent — the context rode on the ProcessingContext, not a thread-local
            assertThat(childThreadFailure.get()).isNull();
            SpanData parent = exportedSpanNamed("Parent");
            SpanData child = exportedSpanNamed("Child");
            assertThat(child.getTraceId()).isEqualTo(parent.getTraceId());
            assertThat(child.getParentSpanContext().getSpanId()).isEqualTo(parent.getSpanId());
        }
    }
}
