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

import io.micrometer.context.ContextAccessor;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.contextpropagation.ObservationAwareSpanThreadLocalAccessor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.tracing.SpanScope;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.function.Predicate;

/**
 * {@link ContextAccessor} that lets Micrometer's context-propagation machinery read the active trace context from an
 * Axon {@link ProcessingContext}. It bridges the binding's active span, carried under the single, framework-generic
 * {@link SpanScope#RESOURCE_KEY}, to the span thread-local key
 * ({@link ObservationAwareSpanThreadLocalAccessor#KEY}), so that
 * {@code ContextSnapshot.captureAll(processingContext)} captures the active span for restoration elsewhere via
 * {@code setThreadLocals()}.
 * <p>
 * <b>Read-only.</b> {@link #writeValues} is a required part of the {@link ContextAccessor} contract but is a no-op:
 * no code path restores a captured span back <em>into</em> a {@link ProcessingContext} (the {@code ProcessingContext}
 * is never the target of a Reactor context restore -- see {@link #writeableType()}), and doing so would require
 * either owning the restored span's lifecycle (which this accessor cannot -- it did not create the span) or a
 * restore-only {@code SpanScope} whose {@code close()} is a deliberate no-op, which is dead code with no way to
 * verify it behaves correctly. Should a genuine restore path emerge, implement {@link #writeValues} against a real
 * integration test exercising it, rather than reintroducing the no-op scope speculatively.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
final class ProcessingContextAccessor implements ContextAccessor<ProcessingContext, ProcessingContext> {

    /**
     * The single key this accessor bridges: the Micrometer span thread-local key, so a captured value restores the
     * active span through the {@link ObservationAwareSpanThreadLocalAccessor}.
     */
    static final Object SPAN_KEY = ObservationAwareSpanThreadLocalAccessor.KEY;

    @Override
    public Class<? extends ProcessingContext> readableType() {
        return ProcessingContext.class;
    }

    @Override
    public void readValues(ProcessingContext source, Predicate<Object> keyPredicate, Map<Object, Object> target) {
        if (keyPredicate.test(SPAN_KEY)) {
            Span span = MicrometerSpanFactory.rawSpanFrom(source);
            if (span != null) {
                target.put(SPAN_KEY, span);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> @Nullable T readValue(ProcessingContext source, Object key) {
        if (SPAN_KEY.equals(key)) {
            return (T) MicrometerSpanFactory.rawSpanFrom(source);
        }
        return null;
    }

    @Override
    public Class<? extends ProcessingContext> writeableType() {
        return ProcessingContext.class;
    }

    @Override
    public ProcessingContext writeValues(Map<Object, Object> valuesToWrite, ProcessingContext context) {
        // No-op: see the class Javadoc for why this accessor is read-only.
        return context;
    }
}
