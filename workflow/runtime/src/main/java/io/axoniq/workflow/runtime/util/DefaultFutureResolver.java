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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Default {@link FutureResolver} that waits until a future completes.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class DefaultFutureResolver extends FutureResolver {

    /**
     * Waits for the future and propagates its failure.
     *
     * @param future future to resolve
     */
    @Override
    public void resolve(@Nonnull CompletableFuture<?> future) {
        Objects.requireNonNull(future, "Future must not be null").join();
    }
}
