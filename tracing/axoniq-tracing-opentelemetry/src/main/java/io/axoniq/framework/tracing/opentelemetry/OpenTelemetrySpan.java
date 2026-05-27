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
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link Span} implementation backed by OpenTelemetry's {@link io.opentelemetry.api.trace.Span}.
 * <p>
 * The span is created lazily: the wrapped {@link SpanBuilder} produces the underlying OpenTelemetry span only when
 * {@link #start()} is invoked. <b>No {@code ThreadLocal} is used.</b> Instead of OpenTelemetry's
 * {@code Span.makeCurrent()} (which pushes the span onto a thread-local), starting the span records its OpenTelemetry
 * {@link Context} as the active context on the {@link ProcessingContext} it was created with (under
 * {@code activeContextKey}); the previously-active context is restored when the returned {@link SpanScope} is closed.
 * Sibling decorators resolve their parent by reading that same resource — so parent/child relationships flow through
 * the {@link ProcessingContext} (the unit-of-work scoped carrier), exactly as the OpenTelemetry team recommends for
 * reactive / non-thread-per-request code. When no {@link ProcessingContext} is available (imperative-edge spans), no
 * active-context tracking is performed.
 * <p>
 * Instances must always be created by the {@link OpenTelemetrySpanFactory}, which resolves the proper parent context
 * before configuring the {@link SpanBuilder}.
 *
 * @author Mateusz Nowak
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
@Internal
public final class OpenTelemetrySpan implements Span {

    private static final Logger logger = LoggerFactory.getLogger(OpenTelemetrySpan.class);

    private final SpanBuilder spanBuilder;
    private final @Nullable ProcessingContext processingContext;
    private final ResourceKey<Context> activeContextKey;
    private final TextMapPropagator textMapPropagator;
    private io.opentelemetry.api.trace.@Nullable Span span = null;

    /**
     * Initializes the span.
     *
     * @param spanBuilder       the builder providing the underlying OpenTelemetry span on {@link #start()}
     * @param processingContext the processing context to record this span's active context on, or {@code null}
     * @param activeContextKey  the resource key under which the active OpenTelemetry context is stored
     * @param textMapPropagator the propagator used to inject this span's context for {@link #propagateContext(Message)}
     */
    OpenTelemetrySpan(SpanBuilder spanBuilder,
                      @Nullable ProcessingContext processingContext,
                      ResourceKey<Context> activeContextKey,
                      TextMapPropagator textMapPropagator) {
        this.spanBuilder = Objects.requireNonNull(spanBuilder, "The SpanBuilder may not be null.");
        this.processingContext = processingContext;
        this.activeContextKey = Objects.requireNonNull(activeContextKey, "The active-context key may not be null.");
        this.textMapPropagator = Objects.requireNonNull(textMapPropagator, "The TextMapPropagator may not be null.");
    }

    @Override
    public SpanScope start() {
        if (span == null) {
            span = spanBuilder.startSpan();
        } else {
            logger.warn("An attempt was made to start span with id [{}] of trace [{}] a second time.",
                        span.getSpanContext().getSpanId(),
                        span.getSpanContext().getTraceId());
        }
        io.opentelemetry.api.trace.Span startedSpan = span;
        if (processingContext == null) {
            return new OpenTelemetrySpanScope(this, startedSpan, null, false);
        }
        Context spanContext = Context.root().with(startedSpan);
        Context previous = processingContext.putResource(activeContextKey, spanContext);
        return new OpenTelemetrySpanScope(this, startedSpan, previous, true);
    }

    @Override
    public Span addAttribute(String key, String value) {
        if (span == null) {
            spanBuilder.setAttribute(key, value);
        } else {
            span.setAttribute(key, value);
        }
        return this;
    }

    @Override
    public Span recordException(Throwable t) {
        if (span == null) {
            logger.warn("An attempt was made to record an exception on a span that was not started yet.", t);
            return this;
        }
        span.recordException(t);
        span.setStatus(StatusCode.ERROR, t.getMessage());
        return this;
    }

    @Override
    public <M extends Message> M propagateContext(M message) {
        if (span == null) {
            return message;
        }
        Map<String, String> propagationEntries = new HashMap<>();
        textMapPropagator.inject(Context.root().with(span), propagationEntries, MetadataContextSetter.INSTANCE);
        if (propagationEntries.isEmpty()) {
            return message;
        }
        //noinspection unchecked
        return (M) message.andMetadata(propagationEntries);
    }

    /**
     * {@link SpanScope} that, on {@link #close()}, restores the previously-active OpenTelemetry context on the
     * {@link ProcessingContext} (without any {@code ThreadLocal}) and then ends the underlying span.
     */
    private final class OpenTelemetrySpanScope implements SpanScope {

        private final Span scopedSpan;
        private final io.opentelemetry.api.trace.Span otelSpan;
        private final @Nullable Context previousActiveContext;
        private final boolean trackedOnContext;

        private OpenTelemetrySpanScope(Span scopedSpan,
                                       io.opentelemetry.api.trace.Span otelSpan,
                                       @Nullable Context previousActiveContext,
                                       boolean trackedOnContext) {
            this.scopedSpan = scopedSpan;
            this.otelSpan = otelSpan;
            this.previousActiveContext = previousActiveContext;
            this.trackedOnContext = trackedOnContext;
        }

        @Override
        public Span span() {
            return scopedSpan;
        }

        @Override
        public void close() {
            if (trackedOnContext && processingContext != null) {
                if (previousActiveContext != null) {
                    processingContext.putResource(activeContextKey, previousActiveContext);
                } else {
                    processingContext.removeResource(activeContextKey);
                }
            }
            otelSpan.end();
        }
    }
}
