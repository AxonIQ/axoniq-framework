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

package io.axoniq.framework.statecontroller.runtime;

import io.axoniq.framework.statecontroller.Outcome;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageStream.Entry;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.WrappedMessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * {@link HandlerEnhancerDefinition} that recognizes state-controlled command handlers by their signature — no
 * dedicated annotation exists. A plain
 * {@link org.axonframework.messaging.commandhandling.annotation.CommandHandler @CommandHandler} method opts into
 * the State Controller by declaring {@link Outcome} (or {@code CompletableFuture<Outcome>}, for a non-blocking
 * body) as its return type; this enhancer wraps every such {@link MessageHandlingMember}, translating the
 * returned outcome into event appends through the in-context
 * {@link org.axonframework.messaging.eventhandling.gateway.EventAppender EventAppender}.
 * <p>
 * On {@link Outcome.Accept Accept}, the accepted events pass the DCB coverage guard and are appended, and
 * {@link Outcome.Accept#result()} is surfaced as the handler's return value (so the command caller receives
 * whatever the decision body chose to expose, or {@code null} when no result was set). On
 * {@link Outcome.Reject Reject}, the audit events are appended through the same appender — they participate in
 * the same {@code EventStoreTransaction} as the rejection — and a
 * {@link org.axonframework.messaging.commandhandling.CommandExecutionException CommandExecutionException}
 * carrying the reason is thrown so the command caller observes the rejection through standard AF5 exception
 * semantics. Both translations run through {@link OutcomeDispatch#apply}.
 * <p>
 * An asynchronous handler returning {@code CompletableFuture<Outcome>} needs no special handling here: AF5's
 * annotated-member machinery already adapts future results into the {@link MessageStream}, so by the time this
 * wrapper observes the entry, its payload is the resolved {@link Outcome}.
 * <p>
 * Discovered by AF5 via {@link java.util.ServiceLoader ServiceLoader}; the registration file lives at
 * {@code META-INF/services/org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition}.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public final class OutcomeHandlerEnhancer implements HandlerEnhancerDefinition {

    @Override
    public <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
        if (!isStateControlledHandler(original)) {
            return original;
        }
        return new OutcomeWrappingMember<>(original);
    }

    private static boolean isStateControlledHandler(MessageHandlingMember<?> member) {
        return member.canHandleMessageType(CommandMessage.class)
                && member.unwrap(Method.class).map(OutcomeHandlerEnhancer::producesOutcome).orElse(false);
    }

    /**
     * Returns whether the given method's return type opts it into state-controlled dispatch: {@link Outcome}
     * itself (or one of its sealed cases), or a {@link CompletableFuture} whose type argument is such a type.
     */
    private static boolean producesOutcome(Method method) {
        Class<?> returnType = method.getReturnType();
        if (Outcome.class.isAssignableFrom(returnType)) {
            return true;
        }
        return CompletableFuture.class.isAssignableFrom(returnType)
                && method.getGenericReturnType() instanceof ParameterizedType parameterized
                && parameterized.getActualTypeArguments().length == 1
                && parameterized.getActualTypeArguments()[0] instanceof Class<?> argument
                && Outcome.class.isAssignableFrom(argument);
    }

    private static final class OutcomeWrappingMember<T> extends WrappedMessageHandlingMember<T> {

        OutcomeWrappingMember(MessageHandlingMember<T> delegate) {
            super(delegate);
        }

        @Override
        public MessageStream<?> handle(Message message, ProcessingContext context, @Nullable T target) {
            CompletableFuture<@Nullable Message> processed = super.handle(message, context, target)
                    .first()
                    .asCompletableFuture()
                    .thenApply(entry -> processStreamEntry(entry, context))
                    .thenApply(e -> e.orElse(null));
            return MessageStream.fromFuture(processed);
        }

        private static Optional<Message> processStreamEntry(@Nullable Entry<? extends Message> entry,
                                                            ProcessingContext context) {
            Object payload = entry == null ? null : entry.message().payload();
            if (!(payload instanceof Outcome outcome)) {
                throw new IllegalStateException(
                        "A state-controlled command handler must produce an Outcome, but got: "
                                + (payload == null ? "null" : payload.getClass().getName())
                );
            }
            Object result = OutcomeDispatch.apply(outcome, context);
            if (result == null) {
                return Optional.empty();
            }
            // Wrap as a CommandResultMessage so the envelope semantics match the declarative
            // StateControllerComponent path.
            return Optional.of(new GenericCommandResultMessage(new MessageType(result.getClass()), result));
        }
    }
}
