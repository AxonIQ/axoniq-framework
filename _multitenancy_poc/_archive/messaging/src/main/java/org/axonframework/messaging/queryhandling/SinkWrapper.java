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

package org.axonframework.messaging.queryhandling;

/**
 * Abstraction interface to bridge old {@code FluxSink} and {@link reactor.core.publisher.Sinks.Many} API with a common
 * API.
 *
 * @param <T> the type of objects the sink accept
 * @author Stefan Dragisic
 * @since 4.5
 */
public interface SinkWrapper<T> {

    /**
     * Wrapper around Sink complete().
     */
    void complete();

    /**
     * Wrapper around Sink next(Object).
     *
     * @param value to be passed to the delegate sink
     */
    void next(T value);

    /**
     * Wrapper around Sink error(Throwable).
     *
     * @param t to be passed to the delegate sink
     */
    void error(Throwable t);
}
