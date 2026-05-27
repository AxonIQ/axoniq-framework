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

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

/**
 * The sole public abstraction for creating tracing {@link Span spans} in AxoniqFramework.
 * <p>
 * A single {@code SpanFactory} is registered against the framework's {@code ComponentRegistry}; the per-concern
 * tracing modules then wrap messaging, modelling and event-sourcing components with delegating tracing decorators
 * that obtain their spans from this factory. There is intentionally no per-bus or per-component {@code SpanFactory}
 * interface — the per-component span shapes (names, kinds, attributes, cross-process metadata propagation) are
 * implementation details of those internal decorators.
 * <p>
 * <b>No {@code ThreadLocal}.</b> A span's parent is never read from a thread-bound "current span". Parents are resolved
 * from (1) the propagated context carried in a {@link Message}'s metadata (cross-thread / cross-process) and (2) the
 * active span recorded on the supplied {@link ProcessingContext} (in-process nesting; see {@link Span#start()}). When
 * neither yields a parent, the span starts a new trace (a root). To force a new trace regardless of any active span,
 * use {@link #createRootSpan(String)}.
 * <p>
 * The default implementation is {@link NoOpSpanFactory}; the OpenTelemetry binding ships
 * {@code OpenTelemetrySpanFactory}. Multiple factories can be composed with {@link MultiSpanFactory}.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
public interface SpanFactory {

    /**
     * Creates a {@link Span} for an outbound (dispatch / producer) operation on the given {@link Message}. The parent
     * is the active span on {@code context} (when present), so a message dispatched from within another traced
     * operation nests under it; otherwise a root span. The {@code context}, when non-{@code null}, is also forwarded
     * to every registered {@link SpanAttributesProvider}.
     *
     * @param operationName the span name
     * @param message       the message the operation acts on
     * @param context       the active processing context, or {@code null} when none is available
     * @return the created span (not yet started)
     */
    Span createDispatchSpan(String operationName, Message message, @Nullable ProcessingContext context);

    /**
     * Creates a {@link Span} for an inbound (handler / consumer) operation on the given {@link Message}. The parent is
     * the tracing context propagated in {@code message}'s metadata (cross-thread / cross-process); when none is
     * present, the active span on {@code context}; when neither is present, a root span. Never reads a thread-bound
     * current span.
     *
     * @param operationName the span name
     * @param message       the message being handled
     * @param context       the active processing context, or {@code null} when none is available
     * @return the created span (not yet started)
     */
    Span createHandlerSpan(String operationName, Message message, @Nullable ProcessingContext context);

    /**
     * Creates a {@link Span} for an inbound (handler / consumer) operation on the given {@link Message}, with an
     * additional link to {@code linkedMessage}'s span context. The link is rendered by APM UIs as a clickable
     * cross-trace navigation (not a parent-of relationship, not an attribute). Implementations MUST extract the
     * propagated context from {@code linkedMessage}'s metadata and attach it as a span link; when no link can be
     * extracted the span is still created without the link, and this method never throws. Parent resolution is as in
     * {@link #createHandlerSpan(String, Message, ProcessingContext)}.
     *
     * @param operationName the span name
     * @param message       the message being handled
     * @param linkedMessage the message whose span context is linked to
     * @param context       the active processing context, or {@code null} when none is available
     * @return the created span (not yet started)
     */
    Span createLinkedHandlerSpan(String operationName, Message message, Message linkedMessage,
                                 @Nullable ProcessingContext context);

    /**
     * Creates a {@link Span} for an internal operation that is not directly tied to a {@link Message}. The parent is
     * the active span on {@code context} (when present), so the internal span nests under the operation that opened it
     * (for example a handler span); otherwise a root span. Non-message attributes are attached by the calling decorator
     * via {@link Span#addAttribute(String, String)}.
     *
     * @param operationName the span name
     * @param context       the active processing context, or {@code null} when none is available
     * @return the created span (not yet started)
     */
    Span createInternalSpan(String operationName, @Nullable ProcessingContext context);

    /**
     * Creates a {@link Span} that always starts a new trace (a root), ignoring any active span when resolving its own
     * parent. Use this for operations that legitimately begin their own trace and must not attach to a stale or
     * unrelated active span — for example an event-processing batch boundary or an out-of-band snapshot operation
     * running on a pooled thread. When {@code context} is non-{@code null}, starting the span still records it as that
     * context's active span, so spans created next with that context nest under this root.
     *
     * @param operationName the span name
     * @param context       the processing context the root should become the active span of, or {@code null}
     * @return the created root span (not yet started)
     */
    Span createRootSpan(String operationName, @Nullable ProcessingContext context);

    /**
     * Registers a {@link SpanAttributesProvider} that contributes attributes to every span this factory produces.
     *
     * @param provider the provider to register
     */
    void registerAttributesProvider(SpanAttributesProvider provider);
}
