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

package org.axonframework.modelling.saga;

import org.axonframework.messaging.core.MessageStream;
import org.axonframework.common.annotation.AnnotationUtils;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.WrappedMessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.lang.reflect.Executable;
import org.jspecify.annotations.Nullable;

/**
 * A {@link HandlerEnhancerDefinition} inspecting the existence of the {@link EndSaga} annotation on
 * {@link MessageHandlingMember}s. If present, the given {@code MessageHandlingMember} will be wrapped in a
 * {@link EndSageMessageHandlingMember}.
 *
 * @author Steven van Beelen
 * @since 4.5
 */
public class EndSagaMessageHandlerDefinition implements HandlerEnhancerDefinition {

    @Override
    public <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
        return original.unwrap(Executable.class)
                       .filter(executable -> AnnotationUtils.isAnnotationPresent(executable, EndSaga.class))
                       .map(e -> (MessageHandlingMember<T>) new EndSageMessageHandlingMember<>(original))
                       .orElse(original);
    }

    /**
     * A {@link WrappedMessageHandlingMember} implementation dedicated towards {@link MessageHandlingMember}s annotated
     * with {@link EndSaga}. After invocation of the {@link #handle(Message, ProcessingContext, Object)} method, the
     * saga is ended through the {@link SagaLifecycle#end()} method, whether the handler succeeded or failed: Axon
     * Framework 4 called {@code SagaLifecycle.end()} in a {@code finally} block, so a throwing {@code @EndSaga}
     * handler ended its saga all the same.
     *
     * @param <T> the entity type wrapped by this {@link MessageHandlingMember}
     */
    public static class EndSageMessageHandlingMember<T> extends WrappedMessageHandlingMember<T> {

        /**
         * Initializes the member using the given {@code delegate}.
         *
         * @param delegate the actual message handling member to delegate to
         */
        protected EndSageMessageHandlingMember(MessageHandlingMember<T> delegate) {
            super(delegate);
        }

        @Override
        public MessageStream<?> handle(Message message, ProcessingContext context, @Nullable T target) {
            // The two callbacks together are Axon Framework 4's finally: onComplete only runs on an error-free stream,
            // so a failed handler ends the saga in the error path instead, with the failure re-emitted untouched.
            return super.handle(message, context, target)
                        .onErrorContinue(failure -> {
                            SagaLifecycle.forContext(context).end();
                            return MessageStream.failed(failure);
                        })
                        .onComplete(() -> SagaLifecycle.forContext(context).end());
        }
    }
}
