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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SpanFactory} that logs span lifecycle events through SLF4J, for development and debugging when no APM
 * backend is available. Span starts and ends are logged at {@code DEBUG}; recorded exceptions at {@code WARN}.
 * <p>
 * This factory performs no context propagation: {@link #propagateContext(Message)} returns the message unchanged.
 * Combine it with the OpenTelemetry factory through {@link MultiSpanFactory} to get both logging and real tracing.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
public final class LoggingSpanFactory implements SpanFactory {

    /**
     * The singleton {@link LoggingSpanFactory} instance.
     */
    public static final LoggingSpanFactory INSTANCE = new LoggingSpanFactory();

    private static final Logger logger = LoggerFactory.getLogger(LoggingSpanFactory.class);

    private LoggingSpanFactory() {
    }

    @Override
    public Span createDispatchSpan(String operationName, Message message, @Nullable ProcessingContext context) {
        return new LoggingSpan(operationName);
    }

    @Override
    public Span createHandlerSpan(String operationName, Message message, @Nullable ProcessingContext context) {
        return new LoggingSpan(operationName);
    }

    @Override
    public Span createLinkedHandlerSpan(String operationName, Message message, Message linkedMessage,
                                        @Nullable ProcessingContext context) {
        return new LoggingSpan(operationName);
    }

    @Override
    public Span createInternalSpan(String operationName, @Nullable ProcessingContext context) {
        return new LoggingSpan(operationName);
    }

    @Override
    public Span createRootSpan(String operationName, @Nullable ProcessingContext context) {
        return new LoggingSpan(operationName);
    }

    @Override
    public void registerAttributesProvider(SpanAttributesProvider provider) {
        // No-op: this factory does not render attributes.
    }

    private static final class LoggingSpan implements Span {

        private final String operationName;

        private LoggingSpan(String operationName) {
            this.operationName = operationName;
        }

        @Override
        public SpanScope start() {
            logger.debug("Starting span: {}", operationName);
            return new LoggingSpanScope(this, operationName);
        }

        @Override
        public Span addAttribute(String key, String value) {
            logger.debug("Span [{}] attribute {}={}", operationName, key, value);
            return this;
        }

        @Override
        public Span recordException(Throwable t) {
            logger.warn("Span [{}] recorded exception", operationName, t);
            return this;
        }

        @Override
        public <M extends Message> M propagateContext(M message) {
            return message;
        }
    }

    private static final class LoggingSpanScope implements SpanScope {

        private final Span span;
        private final String operationName;

        private LoggingSpanScope(Span span, String operationName) {
            this.span = span;
            this.operationName = operationName;
        }

        @Override
        public Span span() {
            return span;
        }

        @Override
        public void close() {
            logger.debug("Ended span: {}", operationName);
        }
    }
}
