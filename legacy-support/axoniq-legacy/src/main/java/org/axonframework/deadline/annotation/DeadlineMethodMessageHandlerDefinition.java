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

package org.axonframework.deadline.annotation;

import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.HandlerAttributes;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.WrappedMessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Implementation of a {@link HandlerEnhancerDefinition} that is used for {@link DeadlineHandler} annotated methods.
 *
 * @author Milan Savic
 * @author Steven van Beelen
 * @since 3.3.0
 */
public class DeadlineMethodMessageHandlerDefinition implements HandlerEnhancerDefinition {

    /**
     * The base priority of a deadline handler declaring a specific deadline name, ranking it above a handler accepting
     * any name.
     */
    private static final int NAMED_DEADLINE_PRIORITY = 10000;
    /**
     * The base priority of a deadline handler accepting any deadline name, ranking it above a plain event handler for
     * the same payload.
     */
    private static final int ANY_DEADLINE_PRIORITY = 1000;

    @Override
    public <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
        return original.<String>attribute(HandlerAttributes.DEADLINE_NAME)
                       .<MessageHandlingMember<T>>map(
                               deadlineName -> new DeadlineMethodMessageHandlingMember<>(original, deadlineName)
                       )
                       .orElse(original);
    }

    private static class DeadlineMethodMessageHandlingMember<T>
            extends WrappedMessageHandlingMember<T>
            implements DeadlineHandlingMember<T> {

        private final String deadlineName;

        private DeadlineMethodMessageHandlingMember(MessageHandlingMember<T> delegate, String deadlineName) {
            super(delegate);
            this.deadlineName = deadlineName;
        }

        @Override
        public boolean canHandle(Message message, ProcessingContext context) {
            return message instanceof DeadlineMessage deadlineMessage
                    && deadlineNameMatch(deadlineMessage)
                    && super.canHandle(message, context);
        }

        private boolean deadlineNameMatch(DeadlineMessage message) {
            return deadlineNameMatchesAll() || deadlineName.equals(message.getDeadlineName());
        }

        private boolean deadlineNameMatchesAll() {
            return deadlineName.isEmpty();
        }

        @Override
        public int priority() {
            int basePriority = deadlineNameMatchesAll() ? ANY_DEADLINE_PRIORITY : NAMED_DEADLINE_PRIORITY;
            return basePriority + Math.min(Integer.MAX_VALUE - basePriority, super.priority());
        }
    }
}
