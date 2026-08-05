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

package io.axoniq.framework.tracing.micrometer.threadlocal;

import io.micrometer.context.ThreadLocalAccessor;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.contextpropagation.ObservationAwareSpanThreadLocalAccessor;
import io.micrometer.tracing.handler.TracingObservationHandler;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

/**
 * Restores manually-created Micrometer {@link Span spans} while preserving a balanced scope stack for nested context
 * snapshots. Reactor automatic context propagation can temporarily clear a context value inside an already-restored
 * span scope. Both setting a span and clearing it therefore push a distinct {@link Tracer.SpanInScope}; every restore
 * pops and closes exactly one scope.
 * <p>
 * This differs deliberately from Micrometer's default accessor, whose clear operation replaces the scope held by the
 * current restore action. A nested clear can consequently discard the scope needed to restore the outer span and
 * trigger a context-propagation assertion in reactive drivers. Keeping the scopes in a LIFO stack preserves the same
 * Micrometer Tracing semantics for synchronous code while making nested Reactor snapshots safe.
 * <p>
 * The accessor uses Micrometer's standard {@link ObservationAwareSpanThreadLocalAccessor#KEY} and retains its
 * Observation-aware read behavior: when the current span is already governed by an {@link Observation}, the
 * Observation accessor remains its sole carrier. A missing raw-span value is therefore only cleared when this
 * accessor already owns a nested scope; otherwise the span restored by the Observation accessor is left untouched.
 * No Observation is created by this accessor.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@Internal
final class MicrometerSpanThreadLocalAccessor implements ThreadLocalAccessor<Span> {

    private final ObservationRegistry observationRegistry;
    private final Tracer tracer;
    private final ThreadLocal<Deque<Tracer.SpanInScope>> scopes = ThreadLocal.withInitial(ArrayDeque::new);

    MicrometerSpanThreadLocalAccessor(ObservationRegistry observationRegistry, Tracer tracer) {
        this.observationRegistry = Objects.requireNonNull(observationRegistry,
                                                          "The ObservationRegistry may not be null.");
        this.tracer = Objects.requireNonNull(tracer, "The Tracer may not be null.");
    }

    @Override
    public Object key() {
        return ObservationAwareSpanThreadLocalAccessor.KEY;
    }

    @Override
    public @Nullable Span getValue() {
        Span currentSpan = tracer.currentSpan();
        Observation currentObservation = observationRegistry.getCurrentObservation();
        if (currentObservation == null) {
            return currentSpan;
        }
        TracingObservationHandler.TracingContext tracingContext =
                currentObservation.getContext().get(TracingObservationHandler.TracingContext.class);
        return currentSpan != null && (tracingContext == null || !currentSpan.equals(tracingContext.getSpan()))
                ? currentSpan
                : null;
    }

    @Override
    public void setValue(Span value) {
        scopes.get().push(tracer.withSpan(Objects.requireNonNull(value, "The Span may not be null.")));
    }

    @Override
    public void setValue() {
        Deque<Tracer.SpanInScope> currentScopes = scopes.get();
        if (currentScopes.isEmpty()) {
            scopes.remove();
            return;
        }
        currentScopes.push(tracer.withSpan(null));
    }

    @Override
    public void restore(Span previousValue) {
        closeCurrentScope();
        Span restoredSpan = tracer.currentSpan();
        if (!Objects.equals(previousValue, restoredSpan)) {
            throw new IllegalStateException("Closing a propagated span scope restored [" + restoredSpan
                                                    + "] instead of the previous Micrometer span [" + previousValue
                                                    + "].");
        }
    }

    @Override
    public void restore() {
        closeCurrentScope();
    }

    private void closeCurrentScope() {
        Deque<Tracer.SpanInScope> currentScopes = scopes.get();
        if (currentScopes.isEmpty()) {
            scopes.remove();
            return;
        }
        currentScopes.pop().close();
        if (currentScopes.isEmpty()) {
            scopes.remove();
        }
    }
}
