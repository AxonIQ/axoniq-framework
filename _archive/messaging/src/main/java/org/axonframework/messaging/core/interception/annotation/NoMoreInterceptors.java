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

package org.axonframework.messaging.core.interception.annotation;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.annotation.AnnotatedHandlerInspector;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * This will implement {@link MessageHandlerInterceptorMemberChain} with no more interceptors. It can be used a default
 * interceptor, for example in the {@link AnnotatedHandlerInspector}.
 *
 * @param <T> the type of the handlers
 * @author Gerard Klijs
 * @since 4.8.0
 */
public class NoMoreInterceptors<T> implements MessageHandlerInterceptorMemberChain<T> {

    /**
     * Creates and returns a new instance
     *
     * @param <T> the type of the handlers
     * @return a new {@link NoMoreInterceptors} instance
     */
    public static <T> MessageHandlerInterceptorMemberChain<T> instance() {
        return new NoMoreInterceptors<>();
    }

    @Deprecated
    @Override
    public Object handleSync(Message message,
                             ProcessingContext context,
                             T target,
                             MessageHandlingMember<? super T> handler) throws Exception {
        return handler.handleSync(message, context, target);
    }

    @Override
    public MessageStream<?> handle(Message message,
                                   ProcessingContext context,
                                   T target,
                                   MessageHandlingMember<? super T> handler) {
        return handler.handle(message, context, target);
    }
}
