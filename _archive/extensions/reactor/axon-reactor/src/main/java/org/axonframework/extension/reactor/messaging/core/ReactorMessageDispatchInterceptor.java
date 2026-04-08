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

package org.axonframework.extension.reactor.messaging.core;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import reactor.core.publisher.Mono;

/**
 * Reactor equivalent of {@link org.axonframework.messaging.core.MessageDispatchInterceptor}.
 * <p>
 * Runs inside the Reactor subscription, giving access to Reactor's context (e.g.,
 * {@code ReactiveSecurityContextHolder}) which is not available in Axon Framework's native
 * {@link org.axonframework.messaging.core.MessageDispatchInterceptor}.
 * <p>
 * Implementations intercept a message before dispatch, potentially enriching it (e.g., adding metadata), and must call
 * {@link ReactorMessageDispatchInterceptorChain#proceed chain.proceed(message, context)} to continue the chain.
 *
 * @param <M> the type of {@link Message} this interceptor handles
 * @author Milan Savic
 * @author Theo Emanuelsson
 * @since 4.4.2
 */
public interface ReactorMessageDispatchInterceptor<M extends Message> {

    /**
     * Intercepts a message before dispatch. Implementations may enrich the message (e.g., add metadata) and must call
     * {@link ReactorMessageDispatchInterceptorChain#proceed chain.proceed(message, context)} to continue the chain.
     *
     * @param message the message being dispatched
     * @param context the active processing context, if any
     * @param chain   the remaining interceptor chain
     * @return a {@link Mono} that completes with the (possibly enriched) message
     */
    Mono<?> interceptOnDispatch(M message, @Nullable ProcessingContext context,
                                ReactorMessageDispatchInterceptorChain<M> chain);
}
