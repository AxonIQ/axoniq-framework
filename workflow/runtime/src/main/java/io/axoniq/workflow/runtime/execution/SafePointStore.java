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
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.util.concurrent.CompletableFuture;

/**
 * Store for the persisted safe point token of a workflow engine.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public interface SafePointStore extends DescribableComponent {

    /**
     * Fetches the currently stored engine safe point tracking token.
     *
     * @return future of stored safe point tracking token, or empty future if none has been stored yet
     */
    CompletableFuture<TrackingToken> fetchSafePointToken();

    /**
     * Stores the given engine safe point tracking token.
     *
     * @param token safe point tracking token to store
     * @return future indicating completion of token storage
     */
    CompletableFuture<Void> storeSafePointToken(@Nonnull TrackingToken token);
}
