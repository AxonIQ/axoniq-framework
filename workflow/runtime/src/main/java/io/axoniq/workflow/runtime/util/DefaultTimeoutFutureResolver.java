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
package io.axoniq.workflow.runtime.util;

import jakarta.annotation.Nonnull;
import org.axonframework.common.FutureUtils;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Default {@link FutureResolver} that waits for future or a default timeout, throwing an exception if the future has
 * not completed.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class DefaultTimeoutFutureResolver extends FutureResolver {

    /**
     * Default timeout for joining a future.
     */
    public static final Duration DEFAULT_JOIN_TIMEOUT = Duration.ofSeconds(30);

    private final Duration timeout;

    /**
     * Constructs a {@link DefaultTimeoutFutureResolver} with a default timeout.
     */
    public DefaultTimeoutFutureResolver() {
        this(DEFAULT_JOIN_TIMEOUT);
    }

    /**
     * Constructs a {@link DefaultTimeoutFutureResolver} with the given timeout.
     *
     * @param timeout timeout to use for future joining
     */
    public DefaultTimeoutFutureResolver(@Nonnull Duration timeout) {
        this.timeout = timeout;
    }

    /**
     * Waits for the future until it completes or the given {@code timeout} elapses, whichever comes first, and
     * propagates its failure.
     *
     * @param future future to resolve
     * @throws TimeoutException if the future does not complete within the given {@code timeout}.
     * @throws Throwable        the unwrapped cause if the future completed exceptionally (exact type preserved).
     */
    @Override
    public void resolve(@Nonnull CompletableFuture<?> future) {
        FutureUtils.joinAndUnwrap(Objects.requireNonNull(future, "Future must not be null"), timeout);
    }
}
