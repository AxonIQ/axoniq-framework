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

package org.axonframework.messaging.core.annotation;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.concurrent.CompletableFuture;

/**
 * ParameterResolver implementation that injects a fixed value. Useful for injecting parameter values that do not rely
 * on information contained in the incoming message itself.
 *
 * @param <T> The type of value resolved by this parameter
 * @author Allard Buijze
 * @since 2.0.0
 */
public class FixedValueParameterResolver<T> implements ParameterResolver<T> {

    private final T value;

    /**
     * Initialize the ParameterResolver to inject the given {@code value} for each incoming message.
     *
     * @param value The value to inject as parameter.
     */
    public FixedValueParameterResolver(T value) {
        this.value = value;
    }

    @Override
    public CompletableFuture<T> resolveParameterValue(ProcessingContext context) {
        return CompletableFuture.completedFuture(value);
    }

    @Override
    public boolean matches(ProcessingContext context) {
        return true;
    }
}
