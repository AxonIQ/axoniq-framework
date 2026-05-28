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
package io.axoniq.workflow.runtime.execution;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory {@link SafePointStore}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class InMemorySafePointStore implements SafePointStore {

    private final AtomicReference<TrackingToken> safePointToken = new AtomicReference<>();

    @Override
    public CompletableFuture<TrackingToken> fetchSafePointToken() {
        return CompletableFuture.completedFuture(safePointToken.get());
    }

    @Override
    public CompletableFuture<Void> storeSafePointToken(@Nonnull TrackingToken token) {
        this.safePointToken.set(Objects.requireNonNull(token, "Safepoint tracking token must not be null"));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("safePointTokenPresent", safePointToken.get() != null);
    }
}
