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

package io.axoniq.framework.tracing;

import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Objects;

/**
 * Binds a {@link Span}'s lifecycle to a {@link ProcessingContext}'s processing phases instead of a {@code try/finally}
 * block, so the span's scope tracks the framework's processing lifecycle correctly across asynchronous and reactive
 * continuations.
 * <p>
 * {@link #bind(Span, ProcessingContext)} starts the span (making it the active span for the binding execution), stores
 * the resulting {@link SpanScope} on the context under {@link #SPAN_SCOPE_KEY} (never via a {@code ThreadLocal}),
 * records any processing error on the span, and closes the scope when the context completes (on both the success and
 * the error path). It is the shared building block the tracing decorators use to wrap handler-side spans, and is
 * available to external decorator authors building their own tracing.
 *
 * @author Mateusz Nowak
 * @since 5.2.0
 */
public final class ProcessingContextSpanBinding {

    /**
     * Resource key under which the active span's {@link SpanScope} is stored on a {@link ProcessingContext}.
     */
    public static final Context.ResourceKey<SpanScope> SPAN_SCOPE_KEY =
            Context.ResourceKey.withLabel("io.axoniq.framework.tracing.SpanScope");

    private ProcessingContextSpanBinding() {
    }

    /**
     * Starts the given span and binds its scope to the given processing context's lifecycle: the scope is stored on
     * the context, the span records any processing error, and the scope is closed once the context completes.
     *
     * @param span    the span to start and bind
     * @param context the processing context to bind the span's lifecycle to
     * @return the started {@link SpanScope}, also stored on the context under {@link #SPAN_SCOPE_KEY}
     */
    public static SpanScope bind(Span span, ProcessingContext context) {
        Objects.requireNonNull(span, "span may not be null");
        Objects.requireNonNull(context, "context may not be null");
        SpanScope scope = span.start();
        context.putResource(SPAN_SCOPE_KEY, scope);
        context.onError((ctx, phase, error) -> span.recordException(error));
        context.doFinally(ctx -> scope.close());
        return scope;
    }
}
