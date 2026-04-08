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

package org.axonframework.messaging.core.annotation;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;

import static org.axonframework.common.annotation.AnnotationUtils.findAnnotationAttributes;

/**
 * The default HandlerDefinition implementation in Axon. It implements the rules of annotated handlers used in all the
 * different types of handlers in Axon.
 * <p>
 * For this implementation to recognize a handler method, it should be (meta)annotated with {@link MessageHandler}. It
 * is recommended to meta-annotated members, and preconfigure the expected {@code messageType}. For example, and event
 * handler should define {@code @MessageHandler(messageType = EventMessage.class)}, indicating that this handler should
 * only be invoked for {@link EventMessage}s.
 * <p>
 * Use {@link HandlerEnhancerDefinition} to add extra behavior or information on top of handlers created by this
 * definition.
 *
 * @author Allard Buijze
 * @see HandlerEnhancerDefinition
 * @see CommandHandler
 * @see EventHandler
 * @since 3.0.0
 */
public class AnnotatedMessageHandlingMemberDefinition implements HandlerDefinition {

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<MessageHandlingMember<T>> createHandler(
            Class<T> declaringType,
            Method method,
            ParameterResolverFactory parameterResolverFactory,
            Function<Object, MessageStream<?>> messageStreamResolver
    ) {
        return findAnnotationAttributes(method, MessageHandler.class)
                .map(attr -> new MethodInvokingMessageHandlingMember<>(
                        method,
                        (Class<? extends Message>) attr.getOrDefault("messageType", Message.class),
                        (Class<? extends Message>) attr.getOrDefault("payloadType", Object.class),
                        parameterResolverFactory,
                        messageStreamResolver
                ));
    }
}
