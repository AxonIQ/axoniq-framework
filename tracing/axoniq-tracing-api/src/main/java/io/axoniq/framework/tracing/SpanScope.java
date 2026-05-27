/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.tracing;

/**
 * The active-span scope returned by {@link Span#start()}. Closing the scope ends the underlying {@link Span}.
 * <p>
 * In framework code a {@code SpanScope} is held on the
 * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} via a
 * {@link org.axonframework.messaging.core.Context.ResourceKey} (never via a {@code ThreadLocal}) and is closed when
 * the context completes; see {@link ProcessingContextSpanBinding}. For imperative code it is closed in a
 * try-with-resources block by the {@link Span#run(Runnable)} family of helpers.
 *
 * @author Mateusz Nowak
 * @author Mitchell Herrijgers
 * @since 4.6.5
 */
public interface SpanScope extends AutoCloseable {

    /**
     * Returns the {@link Span} governed by this scope.
     *
     * @return the span this scope governs
     */
    Span span();

    /**
     * Closes this scope, ending the underlying {@link Span}. Must be invoked exactly once.
     */
    @Override
    void close();
}
