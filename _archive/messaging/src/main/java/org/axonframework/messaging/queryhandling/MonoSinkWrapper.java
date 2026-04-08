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

import reactor.core.publisher.MonoSink;

/**
 * Wrapper around {@link MonoSink}. Since project-reactor is not a required dependency in this Axon version, we need
 * wrappers for backwards compatibility. As soon as dependency is no longer optional, this wrapper should be removed.
 *
 * @param <T> The value type
 * @author Milan Savic
 * @since 3.3
 */
class MonoSinkWrapper<T> {

    private final MonoSink<T> monoSink;

    /**
     * Initializes this wrapper with delegate sink.
     *
     * @param monoSink Delegate sink
     */
    MonoSinkWrapper(MonoSink<T> monoSink) {
        this.monoSink = monoSink;
    }

    /**
     * Wrapper around {@link MonoSink#success(Object)}.
     *
     * @param value to be passed to the delegate sink
     */
    public void success(T value) {
        monoSink.success(value);
    }

    /**
     * Wrapper around {@link MonoSink#error(Throwable)}.
     *
     * @param t to be passed to the delegate sink
     */
    public void error(Throwable t) {
        monoSink.error(t);
    }
}
