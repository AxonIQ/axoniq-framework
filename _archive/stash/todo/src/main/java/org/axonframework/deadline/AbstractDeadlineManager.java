/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.deadline;

import org.axonframework.common.ObjectUtils;
import org.axonframework.common.Registration;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.DefaultMessageDispatchInterceptorChain;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageDispatchInterceptor;
import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.LegacyUnitOfWork;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Abstract implementation of the {@link DeadlineManager} to be implemented by concrete solutions for the
 * DeadlineManager. Provides functionality to perform a call to the DeadlineManager in the a {@link LegacyUnitOfWork}
 * it's 'prepare commit' phase. This #runOnPrepareCommitOrNow(Runnable) functionality is required, as the
 * DeadlineManager schedules a Message which needs to happen on order with the other messages published throughout the
 * system.
 *
 * @author Steven van Beelen
 * @since 3.3
 */
public abstract class AbstractDeadlineManager implements DeadlineManager {

    private final List<MessageDispatchInterceptor<? super DeadlineMessage>> dispatchInterceptors = new CopyOnWriteArrayList<>();
    private final List<MessageHandlerInterceptor<? super DeadlineMessage>> handlerInterceptors = new CopyOnWriteArrayList<>();
    protected MessageTypeResolver messageTypeResolver = new ClassBasedMessageTypeResolver();

    /**
     * Run a given {@code deadlineCall} immediately, or schedule it for the {@link LegacyUnitOfWork} it's 'prepare
     * commit' phase if a UnitOfWork is active. This is required as the DeadlineManager schedule message which we want
     * to happen on order with other message being handled.
     *
     * @param deadlineCall a {@link Runnable} to be executed now or on prepare commit if a {@link LegacyUnitOfWork} is
     *                     active
     */
    protected void runOnPrepareCommitOrNow(Runnable deadlineCall) {
        if (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().onPrepareCommit(unitOfWork -> deadlineCall.run());
        } else {
            deadlineCall.run();
        }
    }

    public Registration registerDispatchInterceptor(
            MessageDispatchInterceptor<? super DeadlineMessage> dispatchInterceptor) {
        dispatchInterceptors.add(dispatchInterceptor);
        return () -> dispatchInterceptors.remove(dispatchInterceptor);
    }

    public Registration registerHandlerInterceptor(
            MessageHandlerInterceptor<DeadlineMessage> handlerInterceptor) {
        handlerInterceptors.add(handlerInterceptor);
        return () -> handlerInterceptors.remove(handlerInterceptor);
    }

    /**
     * Provides a list of registered dispatch interception. Do note that this list is not modifiable, and that changes
     * in the internal structure for dispatch interception will be reflected in this list.
     *
     * @return a list of dispatch interception
     */
    protected List<MessageDispatchInterceptor<? super DeadlineMessage>> dispatchInterceptors() {
        return Collections.unmodifiableList(dispatchInterceptors);
    }

    /**
     * Provides a list of registered handler interception. Do note that this list is not modifiable, and that changes in
     * the internal structure for handler interception will be reflected in this list.
     *
     * @return a list of handler interception
     */
    protected List<MessageHandlerInterceptor<? super DeadlineMessage>> handlerInterceptors() {
        return Collections.unmodifiableList(handlerInterceptors);
    }

    /**
     * Applies registered {@link MessageDispatchInterceptor}s to the given {@code message}.
     *
     * @param message the deadline message to be intercepted
     * @return intercepted message
     */
    @SuppressWarnings("unchecked")
    protected DeadlineMessage processDispatchInterceptors(DeadlineMessage message) {
        // FIXME: reintegrate #3065
        try {
            return new DefaultMessageDispatchInterceptorChain<>(dispatchInterceptors())
                    .proceed(message, null)
                    .first()
                    .<DeadlineMessage>cast()
                    .asCompletableFuture()
                    .get()
                    .message();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Returns the given {@code deadlineName} and {@code messageOrPayload} as a DeadlineMessage which expires at the
     * given {@code expiryTime}. If the {@code messageOrPayload} parameter is of type {@link Message}, a new
     * {@code DeadlineMessage} instance will be created using the payload and meta data of the given message. Otherwise,
     * the given {@code messageOrPayload} is wrapped into a {@code GenericDeadlineMessage} as its payload.
     *
     * @param deadlineName     The name for this {@link DeadlineMessage}.
     * @param messageOrPayload A {@link Message} or payload to wrap as a DeadlineMessage
     * @param expiryTime       The timestamp at which the deadline expires
     * @return a DeadlineMessage using the {@code deadlineName} as its deadline qualifiedName and containing the given
     * {@code messageOrPayload} as the payload
     */
    protected DeadlineMessage asDeadlineMessage(String deadlineName,
                                                       Object messageOrPayload,
                                                       Instant expiryTime) {
        if (messageOrPayload instanceof Message) {
            return new GenericDeadlineMessage(deadlineName,
                                              (Message) messageOrPayload,
                                              () -> expiryTime);
        }
        MessageType type = messageTypeResolver.resolveOrThrow(ObjectUtils.nullSafeTypeOf(messageOrPayload));
        return new GenericDeadlineMessage(
                deadlineName, new GenericMessage(type, messageOrPayload), () -> expiryTime
        );
    }
}
