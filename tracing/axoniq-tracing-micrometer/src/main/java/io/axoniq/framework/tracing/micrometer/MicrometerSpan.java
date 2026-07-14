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

import io.axoniq.framework.tracing.micrometer.metadata.MetadataPropagatorSetter;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.tracing.Span;
import org.axonframework.messaging.tracing.SpanScope;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * {@link Span} implementation backed by Micrometer Tracing's {@link io.micrometer.tracing.Span}.
 * <p>
 * The span is created lazily: the wrapped {@link io.micrometer.tracing.Span.Builder} produces the underlying span
 * only when {@link #start()} is invoked. This class carries no {@link org.axonframework.messaging.core.unitofwork.ProcessingContext}
 * of its own and performs no active-span bookkeeping itself -- nesting is entirely the caller's concern, expressed
 * through the single, framework-generic {@link SpanScope#RESOURCE_KEY}: a lifecycle-covering span is bound via
 * {@link Span#coverLifecycle(org.axonframework.messaging.core.unitofwork.ProcessingContext)} (writes the context's
 * root); a branch-scoped span is started via {@link #start()} and its scope carried inward on a branch via
 * {@link SpanScope#addToContext}. {@link MicrometerSpanFactory} resolves the parent for a new span by reading that
 * same key back off whichever context it is given -- see {@link MicrometerSpanFactory#rawSpanFrom}.
 * <p>
 * The returned scope's {@link SpanScope#within(Supplier)} makes this span
 * {@link Tracer#withSpan(io.micrometer.tracing.Span) ThreadLocal-current} for the duration of the synchronous segment
 * it wraps -- both when invoked by the framework-generic {@code Span.branch*} operations and when used directly. This
 * is the sole, scoped, exception-safe {@code ThreadLocal} write the binding performs (via try-with-resources), so
 * instrumented libraries that read Micrometer's current span (WebClient, datasource-micrometer JDBC) and MDC
 * correlation nest under the Axon span.
 * <p>
 * Instances must always be created by {@link MicrometerSpanFactory}, which resolves the proper parent before
 * configuring the builder.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@Internal
public final class MicrometerSpan implements Span, RawSpanCarrier {

    private static final Logger logger = LoggerFactory.getLogger(MicrometerSpan.class);

    private final io.micrometer.tracing.Span.Builder builder;
    private final Tracer tracer;
    private final Propagator propagator;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    // Volatile: written under start()'s monitor, but read lock-free by other threads that obtain this span through
    // a ProcessingContext resource (e.g. parent resolution on a unit-of-work worker thread).
    private volatile io.micrometer.tracing.@Nullable Span span = null;
    private volatile @Nullable MicrometerSpanScope scope = null;

    /**
     * Initializes the span.
     *
     * @param builder    the builder providing the underlying Micrometer span on {@link #start()}
     * @param tracer     the tracer used to make this span current during {@link SpanScope#within(Supplier)}
     * @param propagator the propagator used to inject this span's context for {@link #propagateContext(Message)}
     */
    MicrometerSpan(io.micrometer.tracing.Span.Builder builder,
                   Tracer tracer,
                   Propagator propagator) {
        this.builder = Objects.requireNonNull(builder, "The Span.Builder may not be null.");
        this.tracer = Objects.requireNonNull(tracer, "The Tracer may not be null.");
        this.propagator = Objects.requireNonNull(propagator, "The Propagator may not be null.");
    }

    @Override
    public synchronized SpanScope start() {
        if (span == null) {
            span = builder.start();
            scope = new MicrometerSpanScope(this, span);
        } else {
            logger.warn("An attempt was made to start span with id [{}] of trace [{}] a second time.",
                        span.context().spanId(),
                        span.context().traceId());
        }
        return Objects.requireNonNull(scope);
    }

    /**
     * Returns the underlying, already-started Micrometer span, for parent resolution by
     * {@link MicrometerSpanFactory#rawSpanFrom}. Internal: not part of the {@link Span} contract. Only ever called
     * through a {@link SpanScope} read back off a context, which by construction only exists once this span has been
     * {@link #start() started} -- so {@code span} is guaranteed non-{@code null} here.
     *
     * @return the underlying, started Micrometer span
     */
    @Internal
    @Override
    public io.micrometer.tracing.Span rawSpan() {
        return Objects.requireNonNull(span, "rawSpan() may only be called after the span has been started.");
    }

    @Override
    public Span addAttribute(String key, String value) {
        if (span == null) {
            builder.tag(key, value);
        } else {
            span.tag(key, value);
        }
        return this;
    }

    @Override
    public Span recordException(Throwable t) {
        if (span == null) {
            logger.warn("An attempt was made to record an exception on a span that was not started yet.", t);
            return this;
        }
        span.error(t);
        return this;
    }

    @Override
    public <M extends Message> M propagateContext(M message) {
        io.micrometer.tracing.Span startedSpan = span;
        if (startedSpan == null) {
            return message;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(startedSpan.context(), carrier, MetadataPropagatorSetter.INSTANCE);
        if (carrier.isEmpty()) {
            return message;
        }
        //noinspection unchecked
        return (M) message.andMetadata(carrier);
    }

    /**
     * {@link SpanScope} that idempotently ends the underlying Micrometer span on {@link #close()} and installs it as
     * current for each synchronous {@link #within(Supplier)} operation. Carries no
     * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} bookkeeping of its own -- nesting rides
     * entirely on {@link SpanScope#RESOURCE_KEY}, written by the caller (see the class-level documentation).
     */
    private final class MicrometerSpanScope implements SpanScope {

        private final Span scopedSpan;
        private final io.micrometer.tracing.Span micrometerSpan;
        private MicrometerSpanScope(Span scopedSpan, io.micrometer.tracing.Span micrometerSpan) {
            this.scopedSpan = scopedSpan;
            this.micrometerSpan = micrometerSpan;
        }

        @Override
        public Span span() {
            return scopedSpan;
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                micrometerSpan.end();
            }
        }

        /**
         * Executes the operation with this scope's span installed as the tracer's current span for the duration of
         * the call -- this binding's implementation choice for scope-bound state: a scoped, exception-safe
         * {@code ThreadLocal} write via {@link Tracer#withSpan(io.micrometer.tracing.Span)}. It is what instrumented
         * libraries observe mid-operation (datasource-micrometer JDBC spans, MDC log correlation) and what Reactor's
         * automatic context propagation snapshots at subscription time.
         *
         * @param operation the operation to execute with this scope's span current
         * @param <T>       the operation's result type
         * @return the value produced by {@code operation}
         */
        @Override
        public <T> T within(Supplier<T> operation) {
            try (Tracer.SpanInScope ignored = tracer.withSpan(micrometerSpan)) {
                return operation.get();
            }
        }
    }
}
