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

package io.axoniq.framework.tracing.slice;

import io.axoniq.framework.tracing.NoOpSpanFactory;
import io.axoniq.framework.tracing.SpanFactory;
import io.axoniq.framework.tracing.attributes.MessageIdSpanAttributesProvider;
import io.axoniq.framework.tracing.attributes.MessageNameSpanAttributesProvider;
import io.axoniq.framework.tracing.attributes.MessageTypeSpanAttributesProvider;
import io.axoniq.framework.tracing.attributes.PayloadTypeSpanAttributesProvider;
import io.axoniq.framework.tracing.messaging.internal.TracingCommandBus;
import io.axoniq.framework.tracing.opentelemetry.OpenTelemetrySpanFactory;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandBusTestUtils;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Slice 1 integration test: a dispatched command, routed through a {@link TracingCommandBus} backed by the real
 * OpenTelemetry SDK, produces a connected dispatch → handle span tree with the expected names, kinds and attributes.
 * Spans are captured with an {@link InMemorySpanExporter} (no Docker required).
 */
class SliceCommandBusTracingIntegrationTest {

    private static final QualifiedName COMMAND_NAME = new QualifiedName("BookRoom");
    private static final String DISPATCH_SPAN = "CommandBus.dispatchCommand BookRoom";
    private static final String HANDLE_SPAN = "CommandBus.handleCommand BookRoom";

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private SpanFactory spanFactory;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                                          .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                                          .build();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                                                         .setTracerProvider(tracerProvider)
                                                         .setPropagators(ContextPropagators.create(
                                                                 W3CTraceContextPropagator.getInstance()))
                                                         .build();
        OpenTelemetrySpanFactory otelFactory = new OpenTelemetrySpanFactory(openTelemetry);
        otelFactory.registerAttributesProvider(new MessageIdSpanAttributesProvider());
        otelFactory.registerAttributesProvider(new MessageNameSpanAttributesProvider());
        otelFactory.registerAttributesProvider(new MessageTypeSpanAttributesProvider());
        otelFactory.registerAttributesProvider(new PayloadTypeSpanAttributesProvider());
        this.spanFactory = otelFactory;
    }

    @AfterEach
    void tearDown() {
        tracerProvider.close();
    }

    @Test
    void dispatchedCommandProducesADispatchAndHandlerSpan() {
        // given
        CommandBus tracingBus = new TracingCommandBus(CommandBusTestUtils.aCommandBus(), spanFactory);
        tracingBus.subscribe(COMMAND_NAME, (command, context) -> MessageStream.just(
                new GenericCommandResultMessage(new MessageType("Result"), "booked")));

        // when
        tracingBus.dispatch(bookRoom(), null).join();

        // then
        await().untilAsserted(() -> assertThat(spanNames()).contains(DISPATCH_SPAN, HANDLE_SPAN));

        SpanData dispatchSpan = spanNamed(DISPATCH_SPAN);
        SpanData handleSpan = spanNamed(HANDLE_SPAN);

        // dispatch span is a producer-side span carrying the message attributes
        assertThat(dispatchSpan.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(attribute(dispatchSpan, "axoniq.message.name")).isEqualTo("BookRoom");
        assertThat(attribute(dispatchSpan, "axoniq.message.type")).isEqualTo("COMMAND");
        assertThat(attribute(dispatchSpan, "axoniq.message.payload_type")).isEqualTo(BookRoom.class.getName());
        assertThat(attribute(dispatchSpan, "axoniq.message.id")).isNotNull();

        // handle span is a consumer-side span, child of the dispatch span (same trace, parent = dispatch)
        assertThat(handleSpan.getKind()).isEqualTo(SpanKind.CONSUMER);
        assertThat(handleSpan.getTraceId()).isEqualTo(dispatchSpan.getTraceId());
        assertThat(handleSpan.getParentSpanContext().getSpanId()).isEqualTo(dispatchSpan.getSpanId());
    }

    @Test
    void noOpSpanFactoryProducesNoSpans() {
        // given a bus wrapped with the no-op factory (mirrors tracing disabled)
        CommandBus tracingBus = new TracingCommandBus(CommandBusTestUtils.aCommandBus(), NoOpSpanFactory.INSTANCE);
        tracingBus.subscribe(COMMAND_NAME, (command, context) -> MessageStream.just(
                new GenericCommandResultMessage(new MessageType("Result"), "booked")));

        // when
        tracingBus.dispatch(bookRoom(), null).join();

        // then
        assertThat(spanExporter.getFinishedSpanItems()).isEmpty();
    }

    private static CommandMessage bookRoom() {
        return new GenericCommandMessage(new MessageType(COMMAND_NAME), new BookRoom("room-42"));
    }

    private List<String> spanNames() {
        return spanExporter.getFinishedSpanItems().stream().map(SpanData::getName).toList();
    }

    private SpanData spanNamed(String name) {
        return spanExporter.getFinishedSpanItems().stream()
                           .filter(span -> span.getName().equals(name))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError("No span named " + name));
    }

    private static String attribute(SpanData span, String key) {
        return span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(key));
    }

    private record BookRoom(String roomId) {

    }
}
