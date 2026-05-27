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

package io.axoniq.framework.tracing.messaging.internal;

import io.axoniq.framework.tracing.ProcessingContextSpanBinding;
import io.axoniq.framework.tracing.Span;
import io.axoniq.framework.tracing.SpanFactory;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.WrappedMessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Executable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link HandlerEnhancerDefinition} that wraps annotation-based message handlers in a tracing {@link Span} named after
 * the handling method (for example {@code "RoomBookingHandler.handle(BookRoom)"}).
 * <p>
 * Currently only {@code @CommandHandler} methods are enhanced; coverage of {@code @EventHandler},
 * {@code @QueryHandler} and {@code @EventSourcingHandler} will be added later. The decision whether to enhance a
 * handler is made at wrap time (before any per-invocation work): handlers that are not traced are returned unchanged,
 * so the reflective span-name machinery is never engaged on their hot path.
 *
 * @author Mateusz Nowak
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
@Internal
public final class TracingHandlerEnhancerDefinition implements HandlerEnhancerDefinition {

    private final SpanFactory spanFactory;

    /**
     * Initializes the enhancer obtaining spans from the given {@code spanFactory}.
     *
     * @param spanFactory the factory producing the tracing spans
     */
    public TracingHandlerEnhancerDefinition(SpanFactory spanFactory) {
        this.spanFactory = Objects.requireNonNull(spanFactory, "spanFactory may not be null");
    }

    @Override
    public <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
        if (!original.canHandleMessageType(CommandMessage.class)) {
            return original;
        }
        Optional<Executable> executable = original.unwrap(Executable.class);
        if (executable.isEmpty()) {
            return original;
        }
        String signature = toMethodSignature(executable.get());
        return new TracingMember<>(original, spanFactory, signature);
    }

    private static String toMethodSignature(Executable executable) {
        return String.format("%s(%s)",
                             executable.getName(),
                             Arrays.stream(executable.getParameterTypes())
                                   .map(Class::getSimpleName)
                                   .collect(Collectors.joining(",")));
    }

    private static final class TracingMember<T> extends WrappedMessageHandlingMember<T> {

        private final SpanFactory spanFactory;
        private final String signature;

        private TracingMember(MessageHandlingMember<T> delegate, SpanFactory spanFactory, String signature) {
            super(delegate);
            this.spanFactory = spanFactory;
            this.signature = signature;
        }

        @Override
        public MessageStream<?> handle(Message message, ProcessingContext context, @Nullable T target) {
            Span span = spanFactory.createInternalSpan(spanName(target), context);
            ProcessingContextSpanBinding.bind(span, context);
            return super.handle(message, context, target);
        }

        private String spanName(@Nullable T target) {
            return target == null ? signature : target.getClass().getSimpleName() + "." + signature;
        }
    }
}
