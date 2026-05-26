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
import io.axoniq.framework.tracing.SpanAttributesProvider;
import io.axoniq.framework.tracing.SpanScope;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests {@link OpenTelemetrySpanFactory} against a real OpenTelemetry SDK using an in-memory span exporter as the
 * recording double, asserting on the exported {@link SpanData}.
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
        void createDispatchSpanAppliesMessageAttributes() {
            // given
            Message message = anEvent();
            factory.registerAttributesProvider(
                    (m, ctx) -> Map.of("axoniq.message.id", m.identifier()));
            Span span = factory.createDispatchSpan("MyDispatch", message, null);

            // when
            try (SpanScope ignored = span.start()) {
                // no-op body
            }

            // then
            SpanData exported = exportedSpan();
            assertThat(exported.getAttributes().asMap())
                    .anySatisfy((key, value) -> {
                        assertThat(key.getKey()).isEqualTo("axoniq.message.id");
                        assertThat(value).isEqualTo(message.identifier());
                    });
        }
    }

    @Nested
    class HandlerSpans {

        @Test
        void propagateContextWritesTraceparentMetadata() {
            // given
            Span dispatchSpan = factory.createDispatchSpan("MyDispatch", anEvent(), null);

            // when
            Message propagated;
            try (SpanScope ignored = dispatchSpan.start()) {
                propagated = factory.propagateContext(anEvent());
            }

            // then
            assertThat(propagated.metadata()).containsKey("traceparent");
        }

        @Test
        void createHandlerSpanParentsOnPropagatedContext() {
            // given
            Span dispatchSpan = factory.createDispatchSpan("MyDispatch", anEvent(), null);
            Message handledMessage;
            String dispatchTraceId;
            String dispatchSpanId;
            try (SpanScope dispatchScope = dispatchSpan.start()) {
                handledMessage = factory.propagateContext(anEvent());
                io.opentelemetry.api.trace.Span currentOtelSpan = io.opentelemetry.api.trace.Span.current();
                dispatchTraceId = currentOtelSpan.getSpanContext().getTraceId();
                dispatchSpanId = currentOtelSpan.getSpanContext().getSpanId();
            }

            // when
            Span handlerSpan = factory.createHandlerSpan("MyHandler", handledMessage, null);
            try (SpanScope ignored = handlerSpan.start()) {
                // no-op body
            }

            // then
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            SpanData handlerSpanData = spans.stream()
                                            .filter(s -> s.getName().equals("MyHandler"))
                                            .findFirst()
                                            .orElseThrow();
            assertThat(handlerSpanData.getKind()).isEqualTo(SpanKind.CONSUMER);
            assertThat(handlerSpanData.getTraceId()).isEqualTo(dispatchTraceId);
            assertThat(handlerSpanData.getParentSpanId()).isEqualTo(dispatchSpanId);
        }
    }

    @Nested
    class InternalSpans {

        @Test
        void createInternalSpanExportsInternalKindSpan() {
            // given
            Span span = factory.createInternalSpan("MyInternal");

            // when
            try (SpanScope ignored = span.start()) {
                // no-op body
            }

            // then
            SpanData exported = exportedSpan();
            assertThat(exported.getName()).isEqualTo("MyInternal");
            assertThat(exported.getKind()).isEqualTo(SpanKind.INTERNAL);
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
            // given
            Span siblingSpan = factory.createDispatchSpan("Sibling", anEvent(), null);
            Message linkedMessage;
            String siblingTraceId;
            try (SpanScope ignored = siblingSpan.start()) {
                linkedMessage = factory.propagateContext(anEvent());
                siblingTraceId = io.opentelemetry.api.trace.Span.current().getSpanContext().getTraceId();
            }

            // when
            Span span = factory.createLinkedHandlerSpan("MyLinked", anEvent(), linkedMessage, null);
            try (SpanScope ignored = span.start()) {
                // no-op body
            }

            // then
            SpanData linkedSpanData = spanExporter.getFinishedSpanItems().stream()
                                                  .filter(s -> s.getName().equals("MyLinked"))
                                                  .findFirst()
                                                  .orElseThrow();
            assertThat(linkedSpanData.getLinks()).hasSize(1);
            assertThat(linkedSpanData.getLinks().get(0).getSpanContext().getTraceId()).isEqualTo(siblingTraceId);
        }
    }

    @Nested
    class Propagation {

        @Test
        void fieldsReportReservedKeys() {
            // when / then
            assertThat(factory.fields()).contains("traceparent");
        }

        @Test
        void propagateContextReturnsSameMessageWhenNoActiveContext() {
            // given
            Message message = anEvent();

            // when
            Message result = factory.propagateContext(message);

            // then
            assertThat(result).isSameAs(message);
        }
    }
}
