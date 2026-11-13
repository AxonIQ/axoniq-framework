/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.util;

import io.axoniq.framework.workflow.runtime.api.execution.FutureResolutionTimeoutException;
import org.axonframework.common.FutureUtils;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Default {@link FutureResolver} that applies a 30-second safety-net timeout to future resolution.
 * <p>
 * Thirty seconds is long enough for legitimate synchronous workflow work, including in-memory, local database, and
 * unit-of-work execution. It is short enough to surface a hung dependency, such as connection-pool exhaustion,
 * deadlock, or network partition, before blocked workflow threads cascade into a broader outage. Callers that
 * legitimately expect a longer wait must configure it explicitly.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class DefaultTimeoutFutureResolver implements FutureResolver {

    /**
     * Default safety-net timeout for joining a future.
     */
    public static final Duration DEFAULT_JOIN_TIMEOUT = Duration.ofSeconds(30);

    private final Duration timeout;

    /**
     * Constructs a  with a default timeout.
     */
    public DefaultTimeoutFutureResolver() {
        this(DEFAULT_JOIN_TIMEOUT);
    }

    /**
     * Constructs a  with the given timeout.
     *
     * @param timeout timeout to use for future joining
     */
    public DefaultTimeoutFutureResolver(Duration timeout) {
        this.timeout = timeout;
    }

    /**
     * Waits for the future until it completes or the configured timeout elapses, whichever comes first.
     * <p>
     * Delegates to {@link FutureUtils#joinAndUnwrap(CompletableFuture, Duration)}. A completed future does not incur
     * timeout work, exceptional completion is rethrown as its original cause, and timeout expiry throws
     * {@link FutureResolutionTimeoutException}. The timeout is applied to a copy so it cannot complete the original
     * asynchronous operation exceptionally.
     *
     * @param future future to resolve
     * @throws FutureResolutionTimeoutException if the future does not complete within the configured timeout
     * @throws Throwable                        the unwrapped cause if the future completed exceptionally (exact type
     *                                          preserved).
     */
    @Override
    public void resolve(CompletableFuture<?> future) {
        try {
            FutureUtils.joinAndUnwrap(Objects.requireNonNull(future, "Future must not be null").copy(), timeout);
        } catch (Exception exception) {
            if (exception instanceof TimeoutException timeoutException) {
                throw new FutureResolutionTimeoutException(timeoutException);
            }
            throw exception;
        }
    }
}
