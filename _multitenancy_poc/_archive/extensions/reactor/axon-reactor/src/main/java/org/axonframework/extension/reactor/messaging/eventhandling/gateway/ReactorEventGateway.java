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

package org.axonframework.extension.reactor.messaging.eventhandling.gateway;

import org.axonframework.common.infra.DescribableComponent;
import org.jspecify.annotations.Nullable;
import org.axonframework.extension.reactor.messaging.core.ReactorMessageDispatchInterceptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;

/**
 * Reactor event gateway that runs dispatch interceptors inside the Reactor subscription.
 * <p>
 * Use this instead of {@link org.axonframework.messaging.eventhandling.gateway.EventGateway} when interceptors need
 * access to Reactor context (e.g., {@code ReactiveSecurityContextHolder}).
 * <p>
 * The interceptor chain is built and executed within the Reactor subscription, ensuring that the Reactor
 * {@link reactor.util.context.Context} is available throughout the entire dispatch pipeline.
 *
 * @author Milan Savic
 * @author Theo Emanuelsson
 * @since 4.4.2
 * @see org.axonframework.messaging.eventhandling.gateway.EventGateway
 * @see ReactorMessageDispatchInterceptor
 */
public interface ReactorEventGateway extends DescribableComponent {

    /**
     * Publishes the given {@code events} within the given {@code context} (if available) and returns a {@link Mono}
     * that completes when publishing is done.
     *
     * @param context the processing context, if any, to publish the given {@code events} in
     * @param events  the events to publish
     * @return a {@link Mono} completing when the events have been published
     */
    default Mono<Void> publish(@Nullable ProcessingContext context, Object... events) {
        return publish(context, Arrays.asList(events));
    }

    /**
     * Publishes the given list of {@code events} within the given {@code context} (if available) and returns a
     * {@link Mono} that completes when publishing is done.
     *
     * @param context the processing context, if any, to publish the given {@code events} in
     * @param events  the events to publish
     * @return a {@link Mono} completing when the events have been published
     */
    Mono<Void> publish(@Nullable ProcessingContext context, List<?> events);

    /**
     * Publishes the given {@code events} and returns a {@link Mono} that completes when publishing is done.
     *
     * @param events the events to publish
     * @return a {@link Mono} completing when the events have been published
     * @see #publish(ProcessingContext, List)
     */
    default Mono<Void> publish(Object... events) {
        return publish(null, Arrays.asList(events));
    }

    /**
     * Publishes the given list of {@code events} and returns a {@link Mono} that completes when publishing is done.
     *
     * @param events the events to publish
     * @return a {@link Mono} completing when the events have been published
     * @see #publish(ProcessingContext, List)
     */
    default Mono<Void> publish(List<?> events) {
        return publish(null, events);
    }

}
