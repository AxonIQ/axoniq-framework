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
 * A {@link SpanFactory} that produces spans which do nothing. This is the default factory when no tracing provider is
 * configured, allowing the tracing decorators to be wired unconditionally while imposing no measurable overhead: every
 * operation reduces to a single dispatch returning shared no-op instances.
 *
 * @author Mateusz Nowak
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public final class NoOpSpanFactory implements SpanFactory {

    /**
     * The singleton {@link NoOpSpanFactory} instance.
     */
    public static final NoOpSpanFactory INSTANCE = new NoOpSpanFactory();

    private static final Span NO_OP_SPAN = new NoOpSpan();
    private static final SpanScope NO_OP_SCOPE = new NoOpSpanScope();

    private NoOpSpanFactory() {
    }

    @Override
    public Span createDispatchSpan(String operationName, Message message, @Nullable ProcessingContext context) {
        return NO_OP_SPAN;
    }

    @Override
    public Span createHandlerSpan(String operationName, Message message, @Nullable ProcessingContext context) {
        return NO_OP_SPAN;
    }

    @Override
    public Span createLinkedHandlerSpan(String operationName, Message message, Message linkedMessage,
                                        @Nullable ProcessingContext context) {
        return NO_OP_SPAN;
    }

    @Override
    public Span createInternalSpan(String operationName, @Nullable ProcessingContext context) {
        return NO_OP_SPAN;
    }

    @Override
    public Span createRootSpan(String operationName, @Nullable ProcessingContext context) {
        return NO_OP_SPAN;
    }

    @Override
    public void registerAttributesProvider(SpanAttributesProvider provider) {
        // No-op: this factory produces no spans, so attributes are never recorded.
    }

    private static final class NoOpSpan implements Span {

        @Override
        public SpanScope start() {
            return NO_OP_SCOPE;
        }

        @Override
        public Span addAttribute(String key, String value) {
            return this;
        }

        @Override
        public Span recordException(Throwable t) {
            return this;
        }

        @Override
        public <M extends Message> M propagateContext(M message) {
            return message;
        }
    }

    private static final class NoOpSpanScope implements SpanScope {

        @Override
        public Span span() {
            return NO_OP_SPAN;
        }

        @Override
        public void close() {
            // No-op.
        }
    }
}
