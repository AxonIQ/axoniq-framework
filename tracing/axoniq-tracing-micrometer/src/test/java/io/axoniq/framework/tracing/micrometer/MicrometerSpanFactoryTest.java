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

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.tracing.Span;
import org.axonframework.messaging.tracing.SpanScope;
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
 * Tests {@link MicrometerSpanFactory} against Micrometer Tracing's OpenTelemetry bridge over a real OpenTelemetry SDK,
 * using an in-memory span exporter as the recording double and asserting on the exported {@link SpanData}.
 * <p>
 * Cross-boundary parenting rides on message metadata (W3C trace context via the {@link Propagator}) and in-process
 * nesting rides on the {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} resource shared by the
 * factory and spans. {@code CurrentTracerContextFallback} verifies the read-only, lowest-priority context installed by
 * external instrumentation.
 */
class MicrometerSpanFactoryTest {

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private OpenTelemetrySdk openTelemetry;
    private Tracer tracer;
    private Propagator propagator;
    private MicrometerSpanFactory factory;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                                          .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                                          .build();
        ContextPropagators contextPropagators = ContextPropagators.create(W3CTraceContextPropagator.getInstance());
        openTelemetry = OpenTelemetrySdk.builder()
                                        .setTracerProvider(tracerProvider)
                                        .setPropagators(contextPropagators)
                                        .build();
        io.opentelemetry.api.trace.Tracer otelTracer = openTelemetry.getTracer("AxoniqFramework");
        tracer = new OtelTracer(otelTracer, new OtelCurrentTraceContext(), event -> {
        });
        propagator = new OtelPropagator(contextPropagators, otelTracer);
        factory = new MicrometerSpanFactory(tracer, propagator);
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
        void createDispatchSpanAppliesMessageAttributesFromConstructorProvidedProvider() {
            // given a factory constructed with an inline SpanAttributesProvider contributing a message attribute
            Message message = anEvent();
            MicrometerSpanFactory factoryWithProvider = new MicrometerSpanFactory(
                    tracer, propagator, List.of((m, c) -> Map.of("axoniq.message.id", m.identifier())));
            Span span = factoryWithProvider.createDispatchSpan("MyDispatch", message, null);

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

            // then — no Micrometer span kind maps to the OpenTelemetry default INTERNAL
            SpanData exported = exportedSpan();
            assertThat(exported.getName()).isEqualTo("MyInternal");
            assertThat(exported.getKind()).isEqualTo(SpanKind.INTERNAL);
        }

        @Test
        void createRootSpanIsRootEvenWhenContextHasActiveSpan() {
            // given
            StubProcessingContext context = new StubProcessingContext();
            Span active = factory.createInternalSpan("Active", context);

            // when
            try (SpanScope activeScope = active.start()) {
                ProcessingContext branch = SpanScope.addToContext(context, activeScope);
                Span root = factory.createRootSpan("MyRoot", branch);
                try (SpanScope rootScope = root.start()) {
                    // no-op body
                }
            }

            // then
            SpanData rootSpanData = exportedSpanNamed("MyRoot");
            assertThat(rootSpanData.getKind()).isEqualTo(SpanKind.INTERNAL);
            assertThat(rootSpanData.getParentSpanContext().isValid()).isFalse();
        }

        @Test
        void createRootSpanLinksBackToActiveSpanOnContextWithoutParentingToIt() {
            // given an active span carried on a branch of the context (the triggering operation)
            StubProcessingContext context = new StubProcessingContext();
            Span active = factory.createInternalSpan("Active", context);

            // when a root span is created against that branch while the active span is open
            try (SpanScope activeScope = active.start()) {
                ProcessingContext branch = SpanScope.addToContext(context, activeScope);
                Span root = factory.createRootSpan("MyRoot", branch);
                try (SpanScope rootScope = root.start()) {
                    // no-op body
                }
            }

            // then the root starts its own trace (no parent, different traceId) but links back to the active span
            SpanData activeSpanData = exportedSpanNamed("Active");
            SpanData rootSpanData = exportedSpanNamed("MyRoot");
            assertThat(rootSpanData.getParentSpanContext().isValid()).isFalse();
            assertThat(rootSpanData.getTraceId()).isNotEqualTo(activeSpanData.getTraceId());
            assertThat(rootSpanData.getLinks()).hasSize(1);
            assertThat(rootSpanData.getLinks().get(0).getSpanContext().getSpanId())
                    .isEqualTo(activeSpanData.getSpanId());
        }

        @Test
        void createRootSpanHasNoLinkWhenContextHasNoActiveSpan() {
            // given a context with no active span
            StubProcessingContext context = new StubProcessingContext();

            // when
            Span root = factory.createRootSpan("MyRoot", context);
            try (SpanScope ignored = root.start()) {
                // no-op body
            }

            // then the root is fully standalone: no parent, no link
            SpanData rootSpanData = exportedSpanNamed("MyRoot");
            assertThat(rootSpanData.getParentSpanContext().isValid()).isFalse();
            assertThat(rootSpanData.getLinks()).isEmpty();
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

        @Test
        void handlerSpanWithoutPropagationMetadataStartsNewRoot() {
            // given a message without trace propagation metadata
            Message message = anEvent();

            // when
            factory.createHandlerSpan("Handler", message, null).start().close();

            // then
            SpanData handler = exportedSpanNamed("Handler");
            assertThat(handler.getParentSpanContext().isValid()).isFalse();
        }
    }

    @Nested
    class InProcessNestingViaProcessingContext {

        @Test
        void closedBranchFallsBackToItsParentWithoutCurrentTracerContext() {
            // given a root scope and a branch retained beyond the branch span's lifetime
            ProcessingContext root = new StubProcessingContext();
            SpanScope rootScope = factory.createInternalSpan("Root", null).start();
            root.putResource(SpanScope.RESOURCE_KEY, rootScope);
            SpanScope branchScope = factory.createInternalSpan("Repository", root).start();
            ProcessingContext escapedBranch = SpanScope.addToContext(root, branchScope);

            // when deferred work resolves a parent after the branch closes, with no current span on this thread
            branchScope.close();
            assertThat(tracer.currentSpan()).isNull();
            factory.createInternalSpan("Deferred", escapedBranch).start().close();
            rootScope.close();

            // then the explicit context chain supplies the root parent rather than the ended repository span
            SpanData rootData = exportedSpanNamed("Root");
            SpanData deferredData = exportedSpanNamed("Deferred");
            assertThat(deferredData.getTraceId()).isEqualTo(rootData.getTraceId());
            assertThat(deferredData.getParentSpanContext().getSpanId()).isEqualTo(rootData.getSpanId());
        }

        @Test
        void spanNestsUnderActiveSpanCarriedOnABranchOfTheSameProcessingContext() {
            // given a shared ProcessingContext; in the single-key branch model an operation-scoped span is started
            // imperatively and its scope carried inward on a branch (SpanScope.addToContext) -- there is no push/pop
            // stack, and plain start() writes nothing to the context at all
            StubProcessingContext context = new StubProcessingContext();
            Span spanA = factory.createInternalSpan("A", context);

            // when B is created against the branch A opened, while A is active
            try (SpanScope scopeA = spanA.start()) {
                ProcessingContext branch = SpanScope.addToContext(context, scopeA);
                Span spanB = factory.createInternalSpan("B", branch);
                try (SpanScope scopeB = spanB.start()) {
                    // no-op body
                }
            }

            // then B nests under A purely through the branch
            SpanData spanAData = exportedSpanNamed("A");
            SpanData spanBData = exportedSpanNamed("B");
            assertThat(spanBData.getTraceId()).isEqualTo(spanAData.getTraceId());
            assertThat(spanBData.getParentSpanContext().getSpanId()).isEqualTo(spanAData.getSpanId());
        }

        @Test
        void siblingBranchesOffTheSameRootDoNotCorruptEachOthersParent() {
            // given a shared root context and two independent operations, each opening its own branch off it (e.g.
            // two events in the same streaming batch) -- a former push/pop stack would have let the later branch's
            // start overwrite what the earlier branch's own children resolve as their parent; a branch cannot be
            // corrupted by a sibling branch started later on the same root
            StubProcessingContext root = new StubProcessingContext();
            Span spanA = factory.createInternalSpan("A", root);
            SpanScope scopeA = spanA.start();
            ProcessingContext branchA = SpanScope.addToContext(root, scopeA);

            Span spanB = factory.createInternalSpan("B", root);
            SpanScope scopeB = spanB.start();

            // when a child is created against branch A's context AFTER B has already started on the shared root
            Span childOfA = factory.createInternalSpan("ChildOfA", branchA);
            try (SpanScope ignored = childOfA.start()) {
                // no-op body
            }
            scopeB.close();
            scopeA.close();

            // then the child still parents under A, not B -- unaffected by B's later start on the shared root
            SpanData spanAData = exportedSpanNamed("A");
            SpanData childData = exportedSpanNamed("ChildOfA");
            assertThat(childData.getParentSpanContext().getSpanId()).isEqualTo(spanAData.getSpanId());
        }

        @Test
        void aSpanCreatedDirectlyAgainstTheRootIsUnaffectedByAnOpenBranch() {
            // given a real (branching) ProcessingContext -- unlike StubProcessingContext, which mutates its
            // resources map in place instead of branching on withResource, masking exactly the isolation this test
            // guards -- and a branch opened off its root (e.g. a per-event handler span mid-batch)
            UnitOfWork unitOfWork = UnitOfWorkTestUtils.aUnitOfWork();
            unitOfWork.executeWithResult(root -> {
                Span spanA = factory.createInternalSpan("A", root);
                SpanScope scopeA = spanA.start();
                SpanScope.addToContext(root, scopeA);

                // when a span is created directly against the root (not the branch) while the branch is still open
                Span spanC = factory.createInternalSpan("C", root);
                try (SpanScope ignored = spanC.start()) {
                    // no-op body
                }
                scopeA.close();
                return CompletableFuture.completedFuture(null);
            }).join();

            // then C is a root span: the branch never wrote anything back onto the shared root context
            SpanData spanCData = exportedSpanNamed("C");
            assertThat(spanCData.getParentSpanContext().isValid()).isFalse();
        }
    }

    @Nested
    class LinkedHandlerSpans {

        @Test
        void createContextParentHandlerSpanParentsToContextAndLinksPublisher() {
            // given a publisher propagated through the event and an independently traced batch context
            Span publisherSpan = factory.createDispatchSpan("Publisher", anEvent(), null);
            Message publishedMessage;
            try (SpanScope ignored = publisherSpan.start()) {
                publishedMessage = publisherSpan.propagateContext(anEvent());
            }
            StubProcessingContext context = new StubProcessingContext();
            Span batchSpan = factory.createRootSpan("Batch", context);

            // when the consumer is created inside the batch context
            try (SpanScope batchScope = batchSpan.start()) {
                ProcessingContext batchContext = SpanScope.addToContext(context, batchScope);
                Span consumer = factory.createContextParentHandlerSpan(
                        "MyContextParent", publishedMessage, batchContext);
                try (SpanScope ignored = consumer.start()) {
                    // no-op body
                }
            }

            // then the batch is the structural parent while the publisher remains a causal link
            SpanData publisher = exportedSpanNamed("Publisher");
            SpanData batch = exportedSpanNamed("Batch");
            SpanData consumer = exportedSpanNamed("MyContextParent");
            assertThat(consumer.getKind()).isEqualTo(SpanKind.CONSUMER);
            assertThat(consumer.getTraceId()).isEqualTo(batch.getTraceId());
            assertThat(consumer.getParentSpanContext().getSpanId()).isEqualTo(batch.getSpanId());
            assertThat(consumer.getLinks()).singleElement()
                                           .extracting(link -> link.getSpanContext().getSpanId())
                                           .isEqualTo(publisher.getSpanId());
        }

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

        @Test
        void createDisconnectedHandlerSpanStartsNewTraceLinkedBackToPublisher() {
            // given a dispatch span propagating its context onto the published message
            Span dispatchSpan = factory.createDispatchSpan("Publisher", anEvent(), null);
            Message publishedMessage;
            try (SpanScope ignored = dispatchSpan.start()) {
                publishedMessage = dispatchSpan.propagateContext(anEvent());
            }
            SpanData publisherSpanData = exportedSpanNamed("Publisher");

            // when a disconnected handler span is created for that message
            Span span = factory.createDisconnectedHandlerSpan("MyDisconnected", publishedMessage, null);
            try (SpanScope ignored = span.start()) {
                // no-op body
            }

            // then it starts a NEW trace (no parent) but links back to the publisher
            SpanData disconnected = exportedSpanNamed("MyDisconnected");
            assertThat(disconnected.getKind()).isEqualTo(SpanKind.CONSUMER);
            assertThat(disconnected.getParentSpanContext().isValid()).isFalse();
            assertThat(disconnected.getTraceId()).isNotEqualTo(publisherSpanData.getTraceId());
            assertThat(disconnected.getLinks()).hasSize(1);
            assertThat(disconnected.getLinks().get(0).getSpanContext().getTraceId())
                    .isEqualTo(publisherSpanData.getTraceId());
        }

        @Test
        void createDisconnectedHandlerSpanWithoutW3cMetadataAddsNoLink() {
            // given a message with no W3C traceparent metadata

            // when
            Span span = factory.createDisconnectedHandlerSpan("MyDisconnected", anEvent(), null);
            try (SpanScope ignored = span.start()) {
                // no-op body
            }

            // then no link, no exception
            SpanData disconnected = exportedSpan();
            assertThat(disconnected.getLinks()).isEmpty();
        }
    }

    @Nested
    class CrossThreadNestingViaRealUnitOfWork {

        /**
         * The definitive no-{@code ThreadLocal} proof: a child span created and started on a <em>different thread</em>
         * than its parent still nests under the parent, purely because both resolve through the same
         * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} of a real {@link UnitOfWork}. A
         * thread-local-based implementation would make the child a root, so this test would fail.
         */
        @Test
        void childSpanOnAnotherThreadNestsUnderParentThroughTheProcessingContext() {
            // given a real UnitOfWork; the parent span is started on the UoW thread
            UnitOfWork unitOfWork = UnitOfWorkTestUtils.aUnitOfWork();
            AtomicReference<Throwable> childThreadFailure = new AtomicReference<>();
            AtomicReference<Boolean> currentTracerContextOnWorkerWasNull = new AtomicReference<>();

            unitOfWork.executeWithResult(processingContext -> {
                Span parent = factory.createInternalSpan("Parent", processingContext);
                SpanScope parentScope = parent.start();
                ProcessingContext branch = SpanScope.addToContext(processingContext, parentScope);
                try {
                    // when the child span is created AND started on a different thread, sharing the parent's branch
                    Thread childThread = new Thread(() -> {
                        try {
                            // No bridge is active on this worker, so the current tracer context is empty.
                            currentTracerContextOnWorkerWasNull.set(tracer.currentTraceContext().context() == null);
                            factory.createInternalSpan("Child", branch).start().close();
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
            assertThat(currentTracerContextOnWorkerWasNull.get()).isTrue();
            SpanData parent = exportedSpanNamed("Parent");
            SpanData child = exportedSpanNamed("Child");
            assertThat(child.getTraceId()).isEqualTo(parent.getTraceId());
            assertThat(child.getParentSpanContext().getSpanId()).isEqualTo(parent.getSpanId());
        }
    }

    @Nested
    class CurrentTracerContextFallback {

        /**
         * When neither the {@code ProcessingContext} resource nor message metadata yields a trace context, the factory
         * consults {@code tracer.currentTraceContext().context()} as a read-only, lowest-priority fallback. This is the
         * current tracer context installed by external instrumentation.
         */
        @Test
        void internalSpanWithNoProcessingContextParentsUnderTheCurrentTracerSpan() {
            // given a span made current by external instrumentation
            io.opentelemetry.api.trace.Span externalSpan =
                    openTelemetry.getTracer("external").spanBuilder("External").startSpan();
            try (io.opentelemetry.context.Scope ignored = externalSpan.makeCurrent()) {
                // when a span is created with no ProcessingContext and no metadata context
                factory.createInternalSpan("Inner", null).start().close();
            } finally {
                externalSpan.end();
            }

            // then the span joins the current tracer context
            SpanData inner = exportedSpanNamed("Inner");
            assertThat(inner.getTraceId()).isEqualTo(externalSpan.getSpanContext().getTraceId());
            assertThat(inner.getParentSpanContext().getSpanId()).isEqualTo(externalSpan.getSpanContext().getSpanId());
        }

        @Test
        void handlerSpanWithoutMetadataOrContextParentsUnderTheCurrentTracerSpan() {
            // given
            io.opentelemetry.api.trace.Span externalSpan =
                    openTelemetry.getTracer("external").spanBuilder("External").startSpan();
            try (io.opentelemetry.context.Scope ignored = externalSpan.makeCurrent()) {
                // when a handler span is created for a message without propagated metadata, outside any context
                factory.createHandlerSpan("Handler", anEvent(), null).start().close();
            } finally {
                externalSpan.end();
            }

            // then
            SpanData handler = exportedSpanNamed("Handler");
            assertThat(handler.getParentSpanContext().getSpanId()).isEqualTo(externalSpan.getSpanContext().getSpanId());
        }

        @Test
        void theProcessingContextResourceStillWinsOverTheCurrentTracerContext() {
            // given an active span on a context branch and a different span made current externally
            StubProcessingContext context = new StubProcessingContext();
            Span outer = factory.createInternalSpan("Outer", context);
            SpanScope outerScope = outer.start();
            ProcessingContext branch = SpanScope.addToContext(context, outerScope);
            io.opentelemetry.api.trace.Span externalSpan =
                    openTelemetry.getTracer("external").spanBuilder("External").startSpan();
            try (io.opentelemetry.context.Scope ignored = externalSpan.makeCurrent()) {
                // when
                factory.createInternalSpan("Inner", branch).start().close();
            } finally {
                externalSpan.end();
                outerScope.close();
            }

            // then the processing context takes precedence over the lowest-priority current tracer context
            SpanData inner = exportedSpanNamed("Inner");
            SpanData outerData = exportedSpanNamed("Outer");
            assertThat(inner.getParentSpanContext().getSpanId()).isEqualTo(outerData.getSpanId());
            assertThat(inner.getTraceId()).isNotEqualTo(externalSpan.getSpanContext().getTraceId());
        }

        @Test
        void rootSpanLinksBackToTheCurrentTracerSpanWithoutParentingToIt() {
            // given
            io.opentelemetry.api.trace.Span externalSpan =
                    openTelemetry.getTracer("external").spanBuilder("External").startSpan();
            try (io.opentelemetry.context.Scope ignored = externalSpan.makeCurrent()) {
                // when a root span is created with no ProcessingContext
                factory.createRootSpan("Root", null).start().close();
            } finally {
                externalSpan.end();
            }

            // then the root starts its own trace but links back to the current tracer span
            SpanData root = exportedSpanNamed("Root");
            assertThat(root.getParentSpanContext().isValid()).isFalse();
            assertThat(root.getTraceId()).isNotEqualTo(externalSpan.getSpanContext().getTraceId());
            assertThat(root.getLinks())
                    .anySatisfy(link -> assertThat(link.getSpanContext().getSpanId())
                            .isEqualTo(externalSpan.getSpanContext().getSpanId()));
        }
    }
}
