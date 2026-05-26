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

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Represents one unit of traced work. One or more spans together form a trace, used to monitor and debug
 * (distributed) applications.
 * <p>
 * A {@code Span} is an abstraction that lets AxoniqFramework offer tracing capabilities without depending on a
 * specific tracing provider. A span is opened by calling {@link #start()}, which both starts the span and makes it
 * the active span for subsequently created spans, and returns a {@link SpanScope}. Closing that scope (via
 * {@link SpanScope#close()}) ends the span. Every {@link #start()} must be paired with exactly one
 * {@link SpanScope#close()}.
 * <p>
 * For imperative-style code the convenience helpers {@link #run(Runnable)}, {@link #runSupplier(Supplier)} and
 * {@link #runSupplierAsync(Supplier)} open and close the scope around the given block. Framework code that has access
 * to a {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} should instead bind the span to the
 * context's lifecycle hooks through {@link ProcessingContextSpanBinding}, so the span scope tracks the framework's
 * processing phases correctly across asynchronous and reactive continuations.
 * <p>
 * Spans are created by a {@link SpanFactory}, which is implemented by the tracing provider of choice (for example the
 * OpenTelemetry binding).
 *
 * @author AxonIQ
 * @see SpanFactory
 * @since 5.2.0
 */
public interface Span {

    /**
     * Starts this span, makes it the active span for the current execution, and returns its {@link SpanScope}. The
     * returned scope MUST be closed exactly once; closing it ends the span.
     *
     * @return the {@link SpanScope} governing this span; never {@code null}
     */
    SpanScope start();

    /**
     * Adds an attribute to the span, providing extra information to the APM tooling. Implementations return
     * {@code this} for fluent chaining.
     *
     * @param key   the attribute key
     * @param value the attribute value
     * @return this span, for fluent interfacing
     */
    Span addAttribute(String key, String value);

    /**
     * Records the given exception against the span and marks the span as errored. This does NOT end the span; the span
     * is ended when its {@link SpanScope} is closed.
     *
     * @param t the exception to record
     * @return this span, for fluent interfacing
     */
    Span recordException(Throwable t);

    /**
     * Starts the span, runs the given block inside its active scope, and ends the span afterwards. Exceptions are
     * recorded on the span and rethrown. The {@link Runnable} runs synchronously on the calling thread.
     *
     * @param runnable the block to run
     */
    default void run(Runnable runnable) {
        try (SpanScope ignored = start()) {
            try {
                runnable.run();
            } catch (Throwable t) {
                recordException(t);
                throw t;
            }
        }
    }

    /**
     * Starts the span, runs the given supplier inside its active scope, ends the span afterwards, and returns the
     * supplied value. Exceptions are recorded on the span and rethrown. The {@link Supplier} runs synchronously on the
     * calling thread.
     *
     * @param supplier the value-producing block to run
     * @param <T>      the supplied value type
     * @return the value produced by {@code supplier}
     */
    default <T> T runSupplier(Supplier<T> supplier) {
        try (SpanScope ignored = start()) {
            try {
                return supplier.get();
            } catch (Throwable t) {
                recordException(t);
                throw t;
            }
        }
    }

    /**
     * Starts the span and runs the given asynchronous supplier inside its active scope; the span is ended when the
     * returned {@link CompletableFuture} completes (normally or exceptionally). A failure of the future is recorded on
     * the span. A synchronous failure of the supplier itself is recorded, the span ended, and the throwable rethrown.
     *
     * @param supplier the block producing the {@link CompletableFuture} to trace
     * @param <T>      the future's result type
     * @return a future that completes with the same result/exception as the supplied future
     */
    default <T> CompletableFuture<T> runSupplierAsync(Supplier<CompletableFuture<T>> supplier) {
        SpanScope scope = start();
        CompletableFuture<T> future;
        try {
            future = supplier.get();
        } catch (Throwable t) {
            recordException(t);
            scope.close();
            throw t;
        }
        return future.whenComplete((result, error) -> {
            if (error != null) {
                recordException(error);
            }
            scope.close();
        });
    }
}
