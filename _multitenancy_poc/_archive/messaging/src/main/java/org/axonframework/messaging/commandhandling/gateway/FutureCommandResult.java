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

package org.axonframework.messaging.commandhandling.gateway;

import org.axonframework.messaging.core.Message;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link CommandResult} that wraps a completable future providing the {@link Message} that represents the result.
 *
 * @author Allard Buijze
 * @since 0.6
 */
public class FutureCommandResult implements CommandResult {

    private final CompletableFuture<? extends Message> completableFuture;

    /**
     * Initializes the CommandResult based on the given {@code result} the completes when the result {@link Message}
     * becomes available
     *
     * @param result The completable future that provides the result message when available
     */
    public FutureCommandResult(CompletableFuture<? extends Message> result) {
        this.completableFuture = Objects.requireNonNull(result, "The result may not be null.");
    }

    @Override
    public CompletableFuture<? extends Message> getResultMessage() {
        return completableFuture;
    }
}
