/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.runtime.util;

import io.axoniq.framework.workflow.runtime.api.execution.FutureResolutionTimeoutException;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Resolves a {@link CompletableFuture} according to a configurable waiting policy.
 * <p>
 * Applications can provide one implementation through Java's {@link java.util.ServiceLoader} mechanism or a
 * configuration component to replace the default resolver. The configured resolver is used wherever workflow code
 * waits for a future. When no component is present, {@link DefaultTimeoutFutureResolver} applies a 30-second
 * safety-net timeout. This is sufficient for
 * synchronous in-memory, local database, and unit-of-work operations while surfacing hung dependencies before they
 * exhaust workflow threads. Callers that legitimately expect a longer wait must configure a resolver with that bound.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@FunctionalInterface
public interface FutureResolver {

    /**
     * Resolves a future through the resolver registered in the processing context.
     * <p>
     * A default resolver is used as a fallback for contexts created in tests or outside configured workflow processing.
     * A resolver timeout is always exposed as a {@link FutureResolutionTimeoutException}; this prevents it from being
     * interpreted as a workflow timeout.
     *
     * @param processingContext context containing the configured resolver
     * @param future            future to resolve
     */
    static void resolve(ProcessingContext processingContext,
                        CompletableFuture<?> future) {
        Objects.requireNonNull(processingContext, "Processing context must not be null");
        Objects.requireNonNull(future, "Future must not be null");
        FutureResolver resolver = null;
        try {
            resolver = processingContext.component(FutureResolver.class);
        } catch (ComponentNotFoundException cnfe) {
            // Contexts created outside workflow configuration have no resolver component.
        }
        try {
            (resolver == null ? new DefaultTimeoutFutureResolver() : resolver).resolve(future);
        } catch (Throwable failure) {
            if (failure instanceof TimeoutException timeoutException) {
                throw new FutureResolutionTimeoutException(timeoutException);
            }
            throwUnchecked(failure);
        }
    }

    /**
     * Resolves the given future according to this resolver's policy.
     * <p>
     * Implementations should throw {@link FutureResolutionTimeoutException} when their resolution timeout expires.
     * The context-based {@link #resolve(ProcessingContext, CompletableFuture)} overload also normalizes a raw
     * {@link TimeoutException} for implementations that cannot yet do so.
     *
     * @param future future to resolve
     */
    void resolve(CompletableFuture<?> future);

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
        throw (T) failure;
    }
}
