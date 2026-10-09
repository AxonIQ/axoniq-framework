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

package org.axonframework.deadline;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.ExecutionException;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.MessageHandlerInterceptorChain;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Delivers a fired deadline to the {@link org.axonframework.messaging.ScopeAware} components that resolve its scope,
 * inside a unit of work from the configured {@link UnitOfWorkFactory}.
 * <p>
 * The registered {@link MessageHandlerInterceptor handler interceptors} run around the delivery, and both get the unit
 * of work's {@link ProcessingContext}, which carries the deadline message. A component that fails to handle the
 * deadline fails the delivery with an {@link ExecutionException}, as in Axon Framework 4, and the unit of work rolls
 * back.
 * <p>
 * This class is internal, as it only serves the deadline managers of this module, which deliver fired deadlines in the
 * same way.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
@Internal
public final class DeadlineDelivery {

    private final UnitOfWorkFactory unitOfWorkFactory;
    private final ScopeAwareProvider scopeAwareProvider;
    private final List<MessageHandlerInterceptor<? super DeadlineMessage>> handlerInterceptors;

    /**
     * Creates a {@code DeadlineDelivery} delivering through the given components.
     *
     * @param unitOfWorkFactory   the factory of the unit of work a fired deadline runs in
     * @param scopeAwareProvider  the provider of the components that may resolve a deadline's scope
     * @param handlerInterceptors the handler interceptors to run around the delivery, read on every delivery
     */
    public DeadlineDelivery(UnitOfWorkFactory unitOfWorkFactory,
                            ScopeAwareProvider scopeAwareProvider,
                            List<MessageHandlerInterceptor<? super DeadlineMessage>> handlerInterceptors) {
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "The UnitOfWorkFactory may not be null.");
        this.scopeAwareProvider =
                Objects.requireNonNull(scopeAwareProvider, "The ScopeAwareProvider may not be null.");
        this.handlerInterceptors =
                Objects.requireNonNull(handlerInterceptors, "The handler interceptors may not be null.");
    }

    /**
     * Delivers the given {@code deadlineMessage} to the components resolving the given {@code deadlineScope}, and waits
     * for the unit of work to complete.
     *
     * @param deadlineMessage the fired deadline
     * @param deadlineScope   the scope the deadline was scheduled for
     * @throws RuntimeException the failure of a handler interceptor or of the delivery, unwrapped from the unit of work
     */
    public void deliver(DeadlineMessage deadlineMessage, ScopeDescriptor deadlineScope) {
        FutureUtils.joinAndUnwrap(
                unitOfWorkFactory.create()
                                 .executeWithResult(context -> chain(deadlineScope)
                                         .proceed(deadlineMessage, Message.addToContext(context, deadlineMessage))
                                         .ignoreEntries()
                                         .asCompletableFuture())
        );
    }

    private MessageHandlerInterceptorChain<DeadlineMessage> chain(ScopeDescriptor deadlineScope) {
        // Interceptors are declared against a super type of DeadlineMessage, so each can handle the deadline being
        // delivered here; narrowing them lets the chain be typed against it.
        @SuppressWarnings("unchecked")
        List<MessageHandlerInterceptor<DeadlineMessage>> narrowed =
                (List<MessageHandlerInterceptor<DeadlineMessage>>) (List<?>) List.copyOf(handlerInterceptors);
        Iterator<MessageHandlerInterceptor<DeadlineMessage>> interceptors = narrowed.iterator();
        return new MessageHandlerInterceptorChain<>() {
            @Override
            public MessageStream<?> proceed(DeadlineMessage message, ProcessingContext context) {
                try {
                    if (interceptors.hasNext()) {
                        return interceptors.next().interceptOnHandle(message, context, this);
                    }
                    send(message, Message.addToContext(context, message), deadlineScope);
                    return MessageStream.empty();
                } catch (Exception e) {
                    return MessageStream.failed(e);
                }
            }
        };
    }

    private void send(DeadlineMessage deadlineMessage, ProcessingContext context, ScopeDescriptor deadlineScope) {
        scopeAwareProvider.provideScopeAwareStream(deadlineScope)
                          .filter(scopeAwareComponent -> scopeAwareComponent.canResolve(deadlineScope))
                          .forEach(scopeAwareComponent -> {
                              try {
                                  scopeAwareComponent.send(deadlineMessage, context, deadlineScope);
                              } catch (Exception e) {
                                  throw new ExecutionException(
                                          "Failed to send a DeadlineMessage for scope ["
                                                  + deadlineScope.scopeDescription() + "]",
                                          e
                                  );
                              }
                          });
    }
}
