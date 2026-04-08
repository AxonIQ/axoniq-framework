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

package org.axonframework.messaging.commandhandling.annotation;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.HandlerAttributes;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.WrappedMessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Optional;

/**
 * Implementation of a {@link HandlerEnhancerDefinition} used for {@link CommandHandler} annotated methods to wrap a
 * {@link MessageHandlingMember} in a {@link CommandHandlingMember} instance.
 * <p>
 * The {@link CommandHandler#commandName()} is used to define the {@link CommandHandlingMember#commandName()} without
 * any fall back.
 *
 * @author Allard Buijze
 * @since 3.0.0
 */
public class MethodCommandHandlerDefinition implements HandlerEnhancerDefinition {

    @Override
    public <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
        Optional<String> optionalRoutingKey = original.attribute(HandlerAttributes.COMMAND_ROUTING_KEY);
        Optional<String> optionalCommandName = original.attribute(HandlerAttributes.COMMAND_NAME);
        return optionalRoutingKey.isPresent() && optionalCommandName.isPresent()
                ? new MethodCommandHandlingMember<>(original,
                                                    optionalRoutingKey.get(),
                                                    optionalCommandName.get())
                : original;
    }

    private static class MethodCommandHandlingMember<T>
            extends WrappedMessageHandlingMember<T>
            implements CommandHandlingMember<T> {

        private final String commandName;
        private final boolean isFactoryHandler;
        private final String routingKey;

        private MethodCommandHandlingMember(MessageHandlingMember<T> delegate,
                                            String routingKeyAttribute,
                                            String commandNameAttribute) {
            super(delegate);
            Executable executable =
                    delegate.unwrap(Executable.class)
                            .orElseThrow(() -> new AxonConfigurationException(
                                    "The @CommandHandler annotation must be put on an Executable "
                                            + "(either directly or as Meta Annotation)"
                            ));
            commandName = commandNameAttribute;
            final boolean factoryMethod = executable instanceof Method && Modifier.isStatic(executable.getModifiers());
            isFactoryHandler = executable instanceof Constructor || factoryMethod;
            routingKey = "".equals(routingKeyAttribute) ? null : routingKeyAttribute;
        }

        @Override
        public boolean canHandle(Message message, ProcessingContext context) {
            return super.canHandle(message, context) && message instanceof CommandMessage;
        }

        @Override
        public String routingKey() {
            return routingKey;
        }

        @Override
        public String commandName() {
            return commandName;
        }

        @Override
        public boolean isFactoryHandler() {
            return isFactoryHandler;
        }
    }
}
