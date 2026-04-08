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
 * Interface for a mechanism that resolves handler method parameter values from a given {@link ProcessingContext}.
 *
 * @param <T> The type of parameter returned by this resolver.
 * @author Allard Buijze
 * @since 2.0.0
 */
public interface ParameterResolver<T> {

    /**
     * Asynchronously resolves the parameter value from the {@code context}.
     *
     * @param context The current processing context.
     * @return A {@link CompletableFuture} that will complete with the parameter value, or completes with {@code null}.
     * @since 5.0.0
     */
    CompletableFuture<T> resolveParameterValue(ProcessingContext context);

    /**
     * Indicates whether this resolver is capable of providing a value for the given {@code context}.
     *
     * @param context The current processing context.
     * @return Returns {@code true} if this resolver can provide a value for the message, otherwise {@code false}.
     */
    boolean matches(ProcessingContext context);

    /**
     * Returns the class of the payload that is supported by this resolver. Defaults to the {@link Object} class
     * indicating that the payload type is irrelevant for this resolver.
     *
     * @return The class of the payload that is supported by this resolver
     */
    default Class<?> supportedPayloadType() {
        return Object.class;
    }
}
