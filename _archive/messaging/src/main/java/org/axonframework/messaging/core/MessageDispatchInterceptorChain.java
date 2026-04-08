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

/**
 * An interceptor chain that manages the flow of a {@link Message} of type {@code M} through a chain of
 * {@link MessageDispatchInterceptor interceptors}.
 * <p>
 * Interceptors may continue processing via this chain by calling the {@link #proceed(Message, ProcessingContext)}
 * method. Alternatively, they can block processing by returning without calling this method.
 *
 * @param <M> The type of {@link Message} intercepted by this chain.
 * @author Allard Buijze
 * @author Steven van Beelen
 * @author Mitchell Herrijgers
 * @author Simon Zambrovski
 * @since 5.0.0
 */
@FunctionalInterface
public interface MessageDispatchInterceptorChain<M extends Message> {

    /**
     * Signals this interceptor chain to continue processing the {@code message}.
     *
     * @param message The message to pass down the chain.
     * @param context The active processing context, if any. Can be used to (e.g.) validate correlation data.
     * @return A {@link MessageStream} containing the result of processing the given {@code message}.
     */
    MessageStream<?> proceed(M message, @Nullable ProcessingContext context);
}
