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

package org.axonframework.modelling.command.inspection;

import org.axonframework.messaging.core.annotation.HandlerAttributes;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.WrappedMessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.modelling.command.CommandHandlerInterceptor;

import java.util.regex.Pattern;

/**
 * Implementation of {@link HandlerEnhancerDefinition} used for {@link CommandHandlerInterceptor} annotated methods.
 *
 * @author Milan Savic
 * @since 3.3
 */
public class MethodCommandHandlerInterceptorDefinition implements HandlerEnhancerDefinition {

    @Override
    public <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
        return original.<String>attribute(HandlerAttributes.COMMAND_NAME_PATTERN)
                       .map(commandNamePattern -> (MessageHandlingMember<T>)
                               new MethodCommandHandlerInterceptorHandlingMember<>(original, commandNamePattern)
                       )
                       .orElse(original);
    }

    private static class MethodCommandHandlerInterceptorHandlingMember<T> extends WrappedMessageHandlingMember<T> {

        private final Pattern commandNamePattern;

        /**
         * Initializes the member using the given {@code delegate}.
         *
         * @param delegate the actual message handling member to delegate to
         */
        private MethodCommandHandlerInterceptorHandlingMember(MessageHandlingMember<T> delegate,
                                                              String commandNamePattern) {
            super(delegate);
            this.commandNamePattern = Pattern.compile(commandNamePattern);
        }

        @Override
        public boolean canHandle(Message message, ProcessingContext context) {
            return super.canHandle(message, context)
                    && commandNamePattern.matcher(message.type().name())
                                         .matches();
        }
    }
}
