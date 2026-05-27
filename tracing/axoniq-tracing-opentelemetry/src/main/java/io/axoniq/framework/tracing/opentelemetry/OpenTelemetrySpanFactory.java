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

import io.axoniq.framework.tracing.MetadataContextPropagator;
import io.axoniq.framework.tracing.Span;
import io.axoniq.framework.tracing.SpanAttributesProvider;
import io.axoniq.framework.tracing.SpanFactory;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@link SpanFactory} implementation backed by OpenTelemetry. OpenTelemetry is a standard for collecting traces,
 * metrics and logs from applications; this factory implements the tracing part by producing {@link OpenTelemetrySpan}
 * instances that delegate to OpenTelemetry's {@link io.opentelemetry.api.trace.Span}.
 * <p>
 * <b>No {@code ThreadLocal}.</b> A span's parent is never read from {@code Context.current()}. It is resolved from
 * (1) the W3C Trace Context propagated in an inbound {@link Message}'s {@link org.axonframework.messaging.core.Metadata}
 * (handler spans) and (2) the OpenTelemetry {@link Context} stored as the active context on the supplied
 * {@link ProcessingContext} under {@link #ACTIVE_CONTEXT_KEY} (in-process nesting; see {@link OpenTelemetrySpan#start()}).
 * The {@code ProcessingContext} is the unit-of-work scoped carrier, so this is the explicit, thread-local-free
 * propagation pattern the OpenTelemetry team recommends for reactive / non-thread-per-request code. Context is
 * propagated onto outbound messages by {@link Span#propagateContext(Message)} (the span injects its own context).
 * <p>
 * To export the collected traces the host application must configure an OpenTelemetry SDK (or run with the
 * OpenTelemetry Java agent); without it OpenTelemetry uses a no-op implementation and no data is emitted. The factory
 * is created either against an explicit {@link OpenTelemetry} instance or against {@link GlobalOpenTelemetry} via the
 * no-argument constructor.
 * <p>
 * The factory also implements {@link MetadataContextPropagator}, exposing the lower-level inject/fields building
 * blocks so external decorator authors can propagate context onto messages that the built-in decorators do not handle.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
public final class OpenTelemetrySpanFactory implements SpanFactory, MetadataContextPropagator {

    private static final String INSTRUMENTATION_NAME = "AxoniqFramework";

    /**
     * Resource key under which the active OpenTelemetry {@link Context} is stored on a {@link ProcessingContext}, used
     * to resolve in-process parent spans without a {@code ThreadLocal}.
     */
    static final ResourceKey<Context> ACTIVE_CONTEXT_KEY =
            ResourceKey.withLabel("io.axoniq.framework.tracing.opentelemetry.activeContext");

    private final Tracer tracer;
    private final TextMapPropagator textMapPropagator;
    private final List<SpanAttributesProvider> spanAttributesProviders = new CopyOnWriteArrayList<>();

    /**
     * Initializes the factory against the {@link GlobalOpenTelemetry} instance. Convenience constructor equivalent to
     * {@code new OpenTelemetrySpanFactory(GlobalOpenTelemetry.get())}.
     */
    public OpenTelemetrySpanFactory() {
        this(GlobalOpenTelemetry.get());
    }

    /**
     * Initializes the factory against the given {@code openTelemetry} instance, obtaining its {@link Tracer} and
     * {@link TextMapPropagator}.
     *
     * @param openTelemetry the OpenTelemetry instance to obtain the tracer and propagators from
     */
    public OpenTelemetrySpanFactory(OpenTelemetry openTelemetry) {
        Objects.requireNonNull(openTelemetry, "The OpenTelemetry instance may not be null.");
        this.tracer = openTelemetry.getTracer(INSTRUMENTATION_NAME);
        this.textMapPropagator = openTelemetry.getPropagators().getTextMapPropagator();
    }

    @Override
    public Span createDispatchSpan(String operationName, Message message, @Nullable ProcessingContext context) {
        SpanBuilder spanBuilder = createSpanBuilder(operationName, SpanKind.PRODUCER).setParent(activeParent(context));
        addMessageAttributes(spanBuilder, message, context);
        return span(spanBuilder, context);
    }

    @Override
    public Span createHandlerSpan(String operationName, Message message, @Nullable ProcessingContext context) {
        SpanBuilder spanBuilder = createSpanBuilder(operationName, SpanKind.CONSUMER).setParent(handlerParent(message, context));
        addMessageAttributes(spanBuilder, message, context);
        return span(spanBuilder, context);
    }

    @Override
    public Span createLinkedHandlerSpan(String operationName, Message message, Message linkedMessage,
                                        @Nullable ProcessingContext context) {
        SpanBuilder spanBuilder = createSpanBuilder(operationName, SpanKind.CONSUMER).setParent(handlerParent(message, context));
        addLink(spanBuilder, linkedMessage);
        addMessageAttributes(spanBuilder, message, context);
        return span(spanBuilder, context);
    }

    @Override
    public Span createInternalSpan(String operationName, @Nullable ProcessingContext context) {
        SpanBuilder spanBuilder = createSpanBuilder(operationName, SpanKind.INTERNAL).setParent(activeParent(context));
        return span(spanBuilder, context);
    }

    @Override
    public Span createRootSpan(String operationName, @Nullable ProcessingContext context) {
        SpanBuilder spanBuilder = createSpanBuilder(operationName, SpanKind.INTERNAL).setParent(Context.root());
        return span(spanBuilder, context);
    }

    @Override
    public void registerAttributesProvider(SpanAttributesProvider provider) {
        Objects.requireNonNull(provider, "The SpanAttributesProvider may not be null.");
        spanAttributesProviders.add(provider);
    }

    @Override
    public Map<String, String> inject(ProcessingContext context) {
        Objects.requireNonNull(context, "The ProcessingContext may not be null.");
        Context activeContext = context.getResource(ACTIVE_CONTEXT_KEY);
        if (activeContext == null) {
            return Map.of();
        }
        Map<String, String> propagationEntries = new HashMap<>();
        textMapPropagator.inject(activeContext, propagationEntries, MetadataContextSetter.INSTANCE);
        return propagationEntries;
    }

    @Override
    public Collection<String> fields() {
        return textMapPropagator.fields();
    }

    /**
     * Resolves the parent for an in-process span: the active OpenTelemetry context on {@code context}, or
     * {@link Context#root()} when none is present. Never reads {@code Context.current()}.
     */
    private static Context activeParent(@Nullable ProcessingContext context) {
        if (context != null) {
            Context activeContext = context.getResource(ACTIVE_CONTEXT_KEY);
            if (activeContext != null) {
                return activeContext;
            }
        }
        return Context.root();
    }

    /**
     * Resolves the parent for a handler span: the context propagated in the message's metadata when present, else the
     * active context on {@code context}, else a root. The extraction base is {@link Context#root()} (not the
     * thread-local current context).
     */
    private Context handlerParent(Message message, @Nullable ProcessingContext context) {
        Context fromMetadata = textMapPropagator.extract(Context.root(), message.metadata(), MetadataContextGetter.INSTANCE);
        if (io.opentelemetry.api.trace.Span.fromContext(fromMetadata).getSpanContext().isValid()) {
            return fromMetadata;
        }
        return activeParent(context);
    }

    private OpenTelemetrySpan span(SpanBuilder spanBuilder, @Nullable ProcessingContext context) {
        return new OpenTelemetrySpan(spanBuilder, context, ACTIVE_CONTEXT_KEY, textMapPropagator);
    }

    private void addLink(SpanBuilder spanBuilder, Message linkedMessage) {
        Context linkedContext =
                textMapPropagator.extract(Context.root(), linkedMessage.metadata(), MetadataContextGetter.INSTANCE);
        SpanContext linkedSpanContext = io.opentelemetry.api.trace.Span.fromContext(linkedContext).getSpanContext();
        if (linkedSpanContext.isValid()) {
            spanBuilder.addLink(linkedSpanContext);
        }
    }

    private void addMessageAttributes(SpanBuilder spanBuilder, Message message, @Nullable ProcessingContext context) {
        spanAttributesProviders.forEach(provider -> provider.provideForMessage(message, context)
                                                            .forEach(spanBuilder::setAttribute));
    }

    private SpanBuilder createSpanBuilder(String name, SpanKind kind) {
        return tracer.spanBuilder(name).setSpanKind(kind);
    }
}
