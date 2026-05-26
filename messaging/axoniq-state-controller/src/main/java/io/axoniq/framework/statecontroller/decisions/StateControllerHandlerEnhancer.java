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

package io.axoniq.framework.statecontroller.decisions;

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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * {@link HandlerEnhancerDefinition} that wraps every {@link MessageHandlingMember} whose underlying
 * {@link Method} carries the {@link StateController @StateController} annotation, translating the returned
 * {@link Decision} into event appends through the in-context
 * {@link org.axonframework.messaging.eventhandling.gateway.EventAppender EventAppender}.
 * <p>
 * On {@link Decision.Accept Accept}, every event in {@link Decision.Accept#events()} is appended via
 * {@link org.axonframework.messaging.eventhandling.gateway.EventAppender#append(java.util.List)
 * EventAppender.append(List)}, and {@link Decision.Accept#result()} is surfaced
 * as the handler's return value (so the command caller receives whatever the decision body chose to expose, or
 * {@code null} when no result was set). On {@link Decision.Reject Reject}, the audit events on
 * {@link Decision.Reject#auditEvents()} are appended through the same {@link org.axonframework.messaging.eventhandling.gateway.EventAppender EventAppender} — they participate
 * in the same {@code EventStoreTransaction} as the rejection — and a
 * {@link org.axonframework.messaging.commandhandling.CommandExecutionException CommandExecutionException}
 * carrying the reason is thrown so the command caller observes the rejection through standard AF5 exception
 * semantics.
 * <p>
 * Discovered by AF5 via {@link java.util.ServiceLoader ServiceLoader}; the registration file lives at
 * {@code META-INF/services/org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition}.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public final class StateControllerHandlerEnhancer implements HandlerEnhancerDefinition {

    @Override
    public <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
        if (!isStateControllerHandler(original)) {
            return original;
        }
        return new StateControllerWrappingMember<>(original);
    }

    private static boolean isStateControllerHandler(MessageHandlingMember<?> member) {
        return member.unwrap(Method.class)
                     .map(m -> m.isAnnotationPresent(StateController.class))
                     .orElse(false);
    }

    private static final class StateControllerWrappingMember<T> extends WrappedMessageHandlingMember<T> {

        StateControllerWrappingMember(MessageHandlingMember<T> delegate) {
            super(delegate);
        }

        /**
         * The deprecated synchronous dispatch entry point is intentionally not supported. State Controllers
         * route through {@link #handle(Message, ProcessingContext, Object)} (the async path) so the Decision
         * post-processing can run inside the {@link MessageStream} pipeline that AF5 5.2+ uses.
         */
        @Override
        public Object handleSync(Message message, ProcessingContext context, @Nullable T target) {
            throw new UnsupportedOperationException(
                    "State Controller handlers are dispatched via the asynchronous "
                            + "MessageHandlingMember#handle(...) path; handleSync(...) is not supported."
            );
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
            Object result = processDecision(payload, context);
            if (result == null) {
                return Optional.empty();
            }
            // Wrap as a CommandResultMessage so the envelope semantics match the declarative
            // StateControllerComponent path (which is also command-handling: @StateController is meta-annotated
            // with @CommandHandler).
            return Optional.of(new GenericCommandResultMessage(new MessageType(result.getClass()), result));
        }

        private static @Nullable Object processDecision(@Nullable Object handlerResult, ProcessingContext context) {
            if (!(handlerResult instanceof Decision decision)) {
                throw new IllegalStateException(
                        "A @StateController handler must return a Decision, but got: "
                                + (handlerResult == null ? "null" : handlerResult.getClass().getName())
                );
            }
            return DecisionDispatch.apply(decision, context);
        }
    }
}
