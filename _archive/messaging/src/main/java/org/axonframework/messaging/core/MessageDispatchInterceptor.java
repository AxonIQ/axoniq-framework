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

package org.axonframework.messaging.core;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.function.Function;

/**
 * Interceptor that allows {@link Message messages} to be intercepted and modified before they are dispatched.
 * <p>
 * This interceptor provides a very early means to alter or reject message, even before any {@link ProcessingContext} is
 * created.
 *
 * @param <M> The message type this interceptor can process.
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @since 2.0.0
 */
public interface MessageDispatchInterceptor<M extends Message> {

    /**
     * Intercepts a given {@code message} on dispatching.
     * <p/>
     * The implementer of this method might want to intercept the message before passing it to the chain (effectively
     * before calling {@link MessageDispatchInterceptorChain#proceed(Message, ProcessingContext)}) or after the chain
     * (by mapping the resulting message by calling {@link MessageStream#mapMessage(Function)}).
     *
     * @param message          The message to intercept on dispatching.
     * @param context          The active processing context, if any. Can be used to (e.g.) validate correlation data.
     * @param interceptorChain The interceptor chain to signal that processing is finished and further interceptors
     *                         should be called.
     * @return The resulting message stream from
     * {@link MessageDispatchInterceptorChain#proceed(Message, ProcessingContext)}.
     */
    MessageStream<?> interceptOnDispatch(M message,
                                         @Nullable ProcessingContext context,
                                         MessageDispatchInterceptorChain<M> interceptorChain);
}
