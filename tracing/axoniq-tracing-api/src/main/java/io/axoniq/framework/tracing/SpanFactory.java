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
 * The default implementation is {@link NoOpSpanFactory}; the OpenTelemetry binding ships
 * {@code OpenTelemetrySpanFactory}. Multiple factories can be composed with {@link MultiSpanFactory}.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
public interface SpanFactory {

    /**
     * Creates a {@link Span} for an outbound (dispatch / producer) operation on the given {@link Message}. The
     * {@code context}, when non-{@code null}, is forwarded to every registered {@link SpanAttributesProvider} so that
     * providers can read per-context resources (for example
     * {@link org.axonframework.messaging.core.LegacyResources#AGGREGATE_IDENTIFIER_KEY}).
     *
     * @param operationName the span name
     * @param message       the message the operation acts on
     * @param context       the active processing context, or {@code null} when none is available
     * @return the created span (not yet started)
     */
    Span createDispatchSpan(String operationName, Message message, @Nullable ProcessingContext context);

    /**
     * Creates a {@link Span} for an inbound (handler / consumer) operation on the given {@link Message}.
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
     * extracted the span is still created without the link, and this method never throws.
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
     * Creates a {@link Span} for an internal operation that is not directly tied to a {@link Message}. Non-message
     * attributes (for example an entity type or identifier for snapshot or repository operations) are attached by the
     * calling decorator via {@link Span#addAttribute(String, String)}.
     *
     * @param operationName the span name
     * @return the created span (not yet started)
     */
    Span createInternalSpan(String operationName);

    /**
     * Propagates the active tracing context (if any) onto the given {@link Message}'s metadata, returning the
     * (possibly new) message that should be dispatched in place of the input. Implementations MUST be idempotent and
     * MUST NOT throw when no context is active.
     *
     * @param message the message to enrich with the active tracing context
     * @param <M>     the message type
     * @return the message carrying the propagated tracing context, or the input message when no context is active
     */
    <M extends Message> M propagateContext(M message);

    /**
     * Registers a {@link SpanAttributesProvider} that contributes attributes to every span this factory produces.
     *
     * @param provider the provider to register
     */
    void registerAttributesProvider(SpanAttributesProvider provider);
}
