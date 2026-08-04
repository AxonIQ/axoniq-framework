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

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.tracing.Span;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.SpanScope;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Test-only {@link SpanFactory} that produces spans which do nothing. Used to exercise code paths that must reject a
 * non-{@link MicrometerSpanFactory} {@link SpanFactory}.
 *
 * @author Mateusz Nowak
 */
public final class NoOpSpanFactory implements SpanFactory {

    /**
     * Shared instance of the no-op factory.
     */
    public static final NoOpSpanFactory INSTANCE = new NoOpSpanFactory();

    private static final Span NO_OP_SPAN = new NoOpSpan();

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
    public Span createContextParentHandlerSpan(String operationName, Message message,
                                               @Nullable ProcessingContext context) {
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
    public Span createDisconnectedHandlerSpan(String operationName, Message message,
                                              @Nullable ProcessingContext context) {
        return NO_OP_SPAN;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        // No-op - not required for testing
    }

    private static final class NoOpSpan implements Span {

        @Override
        public SpanScope start() {
            return new NoOpSpanScope();
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

        private final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public Span span() {
            return NO_OP_SPAN;
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public void close() {
            closed.set(true);
        }

        @Override
        public <T> T within(Supplier<T> operation) {
            return operation.get();
        }
    }
}
