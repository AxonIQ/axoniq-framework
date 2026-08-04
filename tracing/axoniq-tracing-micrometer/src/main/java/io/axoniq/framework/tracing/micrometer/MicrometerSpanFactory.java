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

import io.axoniq.framework.tracing.micrometer.propagator.MetadataPropagatorGetter;
import io.micrometer.tracing.Link;
import io.micrometer.tracing.Span.Kind;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.tracing.Span;
import org.axonframework.messaging.tracing.SpanAttributesProvider;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.SpanScope;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * {@link SpanFactory} implementation backed by Micrometer Tracing. It produces spans that delegate to Micrometer's
 * {@link io.micrometer.tracing.Span}, obtained from an application-configured {@link Tracer} and {@link Propagator}.
 * <p>
 * <b>No {@code ThreadLocal} writes.</b> A span's parent is resolved from (1) the {@link SpanScope} carried on the
 * supplied {@link ProcessingContext} under the single, framework-generic {@link SpanScope#RESOURCE_KEY} (in-process
 * nesting), unwrapped to the Micrometer {@link TraceContext} via
 * {@link io.micrometer.tracing.Span#context()}, and (2) the trace context propagated in an inbound {@link Message}'s
 * metadata (handler spans, via the application {@link Propagator}). As the lowest-priority, read-only fallback, the
 * current tracer context is consulted. The current tracer context is the context installed by external
 * instrumentation and exposed through {@code tracer.currentTraceContext().context()}. Context is propagated onto
 * outbound messages by {@link Span#propagateContext(Message)}. This class never writes a {@code ThreadLocal}.
 * <p>
 * There is no no-argument constructor: Micrometer has no {@code GlobalOpenTelemetry} equivalent. The host application
 * supplies the {@link Tracer} and {@link Propagator} (tests use {@code Tracer.NOOP} / {@code Propagator.NOOP} or a
 * bridge over an in-memory OpenTelemetry SDK).
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
public final class MicrometerSpanFactory implements SpanFactory {

    /**
     * W3C {@code traceparent} header key, used to derive a {@link TraceContext} for span <em>links</em> (which require
     * a concrete {@code TraceContext}, whereas {@link Propagator#extract} yields a pre-parented builder). Links degrade
     * to no-link when a non-W3C propagator is configured.
     */
    private static final String W3C_TRACEPARENT = "traceparent";

    /**
     * Resolves the raw Micrometer {@link io.micrometer.tracing.Span} carried on {@code context} under the
     * framework-generic {@link SpanScope#RESOURCE_KEY} -- the single funnel every reader in this binding goes through
     * (parent resolution here, the thread-local bridge, {@code ProcessingContextAccessor}) so unwrapping a
     * {@link MicrometerSpan} out of the neutral {@link SpanScope} happens in exactly one place. Degrades gracefully to
     * {@code null} when {@code context} is {@code null}, carries no active scope, or carries a scope from a different
     * {@link SpanFactory} (for example a user-composed {@code LoggingSpanFactory}) -- never throws or casts blindly.
     *
     * @param context the processing context to resolve the active raw span from, or {@code null}
     * @return the active raw Micrometer span, or {@code null} when none is present or it is not this binding's
     */
    static io.micrometer.tracing.@Nullable Span rawSpanFrom(@Nullable ProcessingContext context) {
        if (context == null) {
            return null;
        }
        SpanScope active = SpanScope.fromContext(context);
        if (active != null && active.span() instanceof RawSpanCarrier carrier) {
            return carrier.rawSpan();
        }
        return null;
    }

    private final Tracer tracer;
    private final Propagator propagator;
    private final List<SpanAttributesProvider> spanAttributesProviders;

    /**
     * Initializes the factory against the given {@code tracer} and {@code propagator}, without any
     * {@link SpanAttributesProvider SpanAttributesProviders}. Convenience constructor equivalent to
     * {@code new MicrometerSpanFactory(tracer, propagator, List.of())}.
     *
     * @param tracer     the Micrometer tracer producing spans
     * @param propagator the Micrometer propagator injecting/extracting trace context to/from message metadata
     */
    public MicrometerSpanFactory(Tracer tracer, Propagator propagator) {
        this(tracer, propagator, List.of());
    }

    /**
     * Initializes the factory against the given {@code tracer} and {@code propagator}, with the given
     * {@code attributesProviders} contributing attributes to every message-carrying span this factory produces.
     * <p>
     * The provider list is the complete set for this factory's lifetime. When the factory is built by the framework
     * configuration, the list is resolved from the
     * {@link org.axonframework.messaging.tracing.attributes.SpanAttributesProviderRegistry} component.
     *
     * @param tracer              the Micrometer tracer producing spans
     * @param propagator          the Micrometer propagator injecting/extracting trace context to/from message metadata
     * @param attributesProviders the providers contributing attributes to every message-carrying span
     */
    public MicrometerSpanFactory(Tracer tracer, Propagator propagator,
                                 List<SpanAttributesProvider> attributesProviders) {
        this.tracer = Objects.requireNonNull(tracer, "The Tracer may not be null.");
        this.propagator = Objects.requireNonNull(propagator, "The Propagator may not be null.");
        Objects.requireNonNull(attributesProviders, "The SpanAttributesProviders may not be null.");
        this.spanAttributesProviders = List.copyOf(attributesProviders);
    }

    @Override
    public Span createDispatchSpan(String operationName, Message message, @Nullable ProcessingContext context) {
        // Prefer the processing context, then propagated message metadata, and finally the current tracer context.
        io.micrometer.tracing.Span.Builder builder;
        TraceContext resourceParent = resourceContext(context);
        if (resourceParent != null) {
            builder = tracer.spanBuilder().setParent(resourceParent);
        } else if (hasTraceMetadata(message)) {
            builder = propagator.extract(message.metadata(), MetadataPropagatorGetter.INSTANCE);
        } else {
            builder = parentedTo(currentTracerContext());
        }
        return span(builder, operationName, Kind.PRODUCER, message, context);
    }

    @Override
    public Span createHandlerSpan(String operationName, Message message, @Nullable ProcessingContext context) {
        return span(handlerBuilder(message, context), operationName, Kind.CONSUMER, message, context);
    }

    @Override
    public Span createContextParentHandlerSpan(String operationName, Message message,
                                               @Nullable ProcessingContext context) {
        io.micrometer.tracing.Span.Builder builder = parentedTo(processingOrCurrentContext(context));
        addMessageLink(builder, message);
        return span(builder, operationName, Kind.CONSUMER, message, context);
    }

    @Override
    public Span createLinkedHandlerSpan(String operationName, Message message, Message linkedMessage,
                                        @Nullable ProcessingContext context) {
        io.micrometer.tracing.Span.Builder builder = handlerBuilder(message, context);
        addMessageLink(builder, linkedMessage);
        return span(builder, operationName, Kind.CONSUMER, message, context);
    }

    @Override
    public Span createInternalSpan(String operationName, @Nullable ProcessingContext context) {
        // No span kind renders as an INTERNAL span.
        return span(parentedTo(processingOrCurrentContext(context)), operationName, null, null, context);
    }

    @Override
    public Span createRootSpan(String operationName, @Nullable ProcessingContext context) {
        io.micrometer.tracing.Span.Builder builder = tracer.spanBuilder().setNoParent();
        addProcessingOrCurrentContextLink(builder, context);
        return span(builder, operationName, null, null, context);
    }

    @Override
    public Span createDisconnectedHandlerSpan(String operationName, Message message,
                                              @Nullable ProcessingContext context) {
        // A new root trace linked back to the publisher's context extracted from the message metadata.
        io.micrometer.tracing.Span.Builder builder = tracer.spanBuilder().setNoParent();
        addMessageLink(builder, message);
        return span(builder, operationName, Kind.CONSUMER, message, context);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tracer", tracer);
        descriptor.describeProperty("propagator", propagator);
        descriptor.describeProperty("spanAttributesProviders", spanAttributesProviders);
    }

    /**
     * Resolves the parent builder for a handler span: the context propagated in the message's metadata when present
     * (via the app {@link Propagator}, yielding a pre-parented builder), else a fresh builder parented to the active
     * context on {@code context}, else the current tracer context, else a root.
     */
    private io.micrometer.tracing.Span.Builder handlerBuilder(Message message, @Nullable ProcessingContext context) {
        if (hasTraceMetadata(message)) {
            return propagator.extract(message.metadata(), MetadataPropagatorGetter.INSTANCE);
        }
        return parentedTo(processingOrCurrentContext(context));
    }

    /**
     * Builds a fresh span builder parented to the given {@code parent}, or an explicit no-parent root when
     * {@code parent} is {@code null}. Setting the parent explicitly is required: the Micrometer OpenTelemetry bridge
     * otherwise defaults an un-parented builder to its implicit current context.
     */
    private io.micrometer.tracing.Span.Builder parentedTo(@Nullable TraceContext parent) {
        io.micrometer.tracing.Span.Builder builder = tracer.spanBuilder();
        return parent != null ? builder.setParent(parent) : builder.setNoParent();
    }

    /**
     * Resolves the in-process parent: the active span's context on {@code context}, else the current tracer context.
     */
    private @Nullable TraceContext processingOrCurrentContext(@Nullable ProcessingContext context) {
        TraceContext resourceParent = resourceContext(context);
        return resourceParent != null ? resourceParent : currentTracerContext();
    }

    /**
     * Derives the parent {@link TraceContext} from the active Micrometer span carried on {@code context} (via
     * {@link #rawSpanFrom}), or {@code null} when none is present.
     */
    private @Nullable TraceContext resourceContext(@Nullable ProcessingContext context) {
        io.micrometer.tracing.Span current = rawSpanFrom(context);
        return current != null ? current.context() : null;
    }

    /**
     * Returns the read-only current tracer context, or {@code null} when external instrumentation has not installed
     * one. This is the lowest-priority parent source.
     */
    private @Nullable TraceContext currentTracerContext() {
        return tracer.currentTraceContext().context();
    }

    private boolean hasTraceMetadata(Message message) {
        for (String field : propagator.fields()) {
            if (message.metadata().containsKey(field)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Attaches the processing or current tracer context to {@code builder} as a span <em>link</em>, never as its
     * parent, so a root span stays navigable back to the operation that triggered it.
     */
    private void addProcessingOrCurrentContextLink(io.micrometer.tracing.Span.Builder builder,
                                                   @Nullable ProcessingContext context) {
        TraceContext linked = processingOrCurrentContext(context);
        if (linked != null) {
            builder.addLink(new Link(linked));
        }
    }

    /**
     * Links {@code builder} to the trace context propagated in {@code message}'s metadata as a W3C {@code traceparent}.
     * Degrades to no link when the metadata carries no W3C {@code traceparent} (for example a non-W3C propagator).
     */
    private void addMessageLink(io.micrometer.tracing.Span.Builder builder, Message message) {
        TraceContext linked = traceContextFrom(message);
        if (linked != null) {
            builder.addLink(new Link(linked));
        }
    }

    /**
     * Parses a W3C {@code traceparent} ({@code "00-<traceId>-<spanId>-<flags>"}) from the message metadata into a
     * {@link TraceContext}, or {@code null} when absent or not W3C-formatted.
     */
    private @Nullable TraceContext traceContextFrom(Message message) {
        String traceparent = message.metadata().get(W3C_TRACEPARENT);
        if (traceparent == null) {
            return null;
        }
        String[] parts = traceparent.split("-");
        if (parts.length != 4) {
            return null;
        }
        return tracer.traceContextBuilder()
                     .traceId(parts[1])
                     .spanId(parts[2])
                     .sampled(!"00".equals(parts[3]))
                     .build();
    }

    private Span span(io.micrometer.tracing.Span.Builder builder,
                      String operationName,
                      @Nullable Kind kind,
                      @Nullable Message message,
                      @Nullable ProcessingContext context) {
        builder.name(operationName);
        if (kind != null) {
            builder.kind(kind);
        }
        if (message != null) {
            addMessageAttributes(builder, message, context);
        }
        return new MicrometerSpan(builder, tracer, propagator);
    }

    private void addMessageAttributes(io.micrometer.tracing.Span.Builder builder, Message message,
                                      @Nullable ProcessingContext context) {
        spanAttributesProviders.forEach(provider -> provider.provideForMessage(message, context)
                                                            .forEach(builder::tag));
    }
}
