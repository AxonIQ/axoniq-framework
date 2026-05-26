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
import io.opentelemetry.context.Scope;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * {@link Span} implementation backed by OpenTelemetry's {@link io.opentelemetry.api.trace.Span}.
 * <p>
 * The span is created lazily: the wrapped {@link SpanBuilder} produces the underlying OpenTelemetry span only when
 * {@link #start()} is invoked. Starting the span also makes it the current span for the calling execution and returns
 * a {@link SpanScope}; closing that scope first detaches the OpenTelemetry {@link Scope} and then ends the span, in
 * line with the {@link SpanScope} contract.
 * <p>
 * Instances must always be created by the {@link OpenTelemetrySpanFactory}, which extracts the proper parent context
 * before configuring the {@link SpanBuilder}.
 * <p>
 * This type is {@link Internal} because it is an implementation detail of the OpenTelemetry binding; users interact
 * with it only through the {@link Span} abstraction.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@Internal
public final class OpenTelemetrySpan implements Span {

    private static final Logger logger = LoggerFactory.getLogger(OpenTelemetrySpan.class);

    private final SpanBuilder spanBuilder;
    private io.opentelemetry.api.trace.@Nullable Span span = null;

    /**
     * Initializes the span with the given {@code spanBuilder}, which supplies the underlying OpenTelemetry span when
     * {@link #start()} is invoked.
     *
     * @param spanBuilder the builder providing the underlying OpenTelemetry span
     */
    public OpenTelemetrySpan(SpanBuilder spanBuilder) {
        this.spanBuilder = Objects.requireNonNull(spanBuilder, "The SpanBuilder may not be null.");
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
        Scope scope = startedSpan.makeCurrent();
        return new OpenTelemetrySpanScope(this, startedSpan, scope);
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

    /**
     * {@link SpanScope} implementation that, on {@link #close()}, closes the OpenTelemetry {@link Scope} and then ends
     * the underlying span.
     */
    private record OpenTelemetrySpanScope(Span span,
                                          io.opentelemetry.api.trace.Span otelSpan,
                                          Scope scope) implements SpanScope {

        @Override
        public void close() {
            scope.close();
            otelSpan.end();
        }
    }
}
