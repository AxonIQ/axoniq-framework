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
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandBusTestUtils;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

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

    @Test
    void handlerExecutedOnADifferentThreadStillNestsUnderTheDispatchSpan() {
        // given a command bus whose (tracing-wrapped) handler runs on a SEPARATE thread, inside a real UnitOfWork
        ExecutorService handlerExecutor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "handler-thread"));
        AtomicReference<String> handlerThreadName = new AtomicReference<>();
        String dispatchThreadName = Thread.currentThread().getName();
        try {
            CommandBus tracingBus =
                    new TracingCommandBus(new AsyncHandlerCommandBus(handlerExecutor, handlerThreadName), spanFactory);
            tracingBus.subscribe(COMMAND_NAME, (command, context) -> MessageStream.just(
                    new GenericCommandResultMessage(new MessageType("Result"), "booked")));

            // when
            tracingBus.dispatch(bookRoom(), null).join();
        } finally {
            handlerExecutor.shutdownNow();
        }

        // then the handler really ran on another thread ...
        assertThat(handlerThreadName.get()).isNotNull().isNotEqualTo(dispatchThreadName);

        // ... yet the handle span still nests under the dispatch span — the trace context rode on the command's
        // metadata (and the handler span's own parent on its UnitOfWork's ProcessingContext), never on a ThreadLocal.
        await().untilAsserted(() -> assertThat(spanNames()).contains(DISPATCH_SPAN, HANDLE_SPAN));
        SpanData dispatchSpan = spanNamed(DISPATCH_SPAN);
        SpanData handleSpan = spanNamed(HANDLE_SPAN);
        assertThat(handleSpan.getTraceId()).isEqualTo(dispatchSpan.getTraceId());
        assertThat(handleSpan.getParentSpanContext().getSpanId()).isEqualTo(dispatchSpan.getSpanId());
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

    /**
     * A {@link CommandBus} that runs the subscribed handler on a dedicated executor thread, inside its own real
     * {@link UnitOfWork}, to simulate a dispatch thread and a handler thread that differ (as a distributed bus would).
     */
    private static final class AsyncHandlerCommandBus implements CommandBus {

        private final ExecutorService executor;
        private final AtomicReference<String> handlerThreadName;
        private final Map<QualifiedName, CommandHandler> handlers = new ConcurrentHashMap<>();

        private AsyncHandlerCommandBus(ExecutorService executor, AtomicReference<String> handlerThreadName) {
            this.executor = executor;
            this.handlerThreadName = handlerThreadName;
        }

        @Override
        public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                                @Nullable ProcessingContext processingContext) {
            CommandHandler handler = handlers.get(command.type().qualifiedName());
            return CompletableFuture.supplyAsync(() -> {
                                        handlerThreadName.set(Thread.currentThread().getName());
                                        UnitOfWork unitOfWork = UnitOfWorkTestUtils.aUnitOfWork();
                                        return unitOfWork.executeWithResult(
                                                handlerContext -> handler.handle(command, handlerContext)
                                                                         .first()
                                                                         .asCompletableFuture()
                                        ).join();
                                    }, executor)
                                    .thenApply(entry -> entry == null ? null : entry.message());
        }

        @Override
        public CommandBus subscribe(QualifiedName name, CommandHandler commandHandler) {
            handlers.put(name, commandHandler);
            return this;
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            // not relevant to this test
        }
    }
}
