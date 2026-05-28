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
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * {@link SafePointStore} backed by an Axon {@link TokenStore}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class TokenStoreSafePointStore implements SafePointStore {

    private static final int SEGMENT_ID = 0;

    private final TokenStore tokenStore;
    private final String tokenStoreIdentifier;

    /**
     * Creates a token-store backed engine safe point tracking token store.
     *
     * @param tokenStore           token store to persist tracking tokens in
     * @param tokenStoreIdentifier identifier used inside the token store
     */
    public TokenStoreSafePointStore(@Nonnull TokenStore tokenStore,
                                    @Nonnull String tokenStoreIdentifier) {
        this.tokenStore = Objects.requireNonNull(tokenStore, "tokenStore must not be null");
        this.tokenStoreIdentifier = Objects.requireNonNull(tokenStoreIdentifier,
                                                           "tokenStoreIdentifier must not be null");
    }

    /**
     * Builds the token-store identifier used for safe point persistence for the given workflow module name.
     *
     * @param moduleName workflow module name
     * @return token-store identifier
     */
    public static String tokenStoreIdentifier(@Nonnull String moduleName) {
        return moduleName + "SafePoint";
    }

    @Override
    public CompletableFuture<TrackingToken> fetchSafePointToken() {
        return tokenStore.fetchSegments(tokenStoreIdentifier, null)
                         .thenCompose(segments -> {
                             if (segments.isEmpty()) {
                                 return CompletableFuture.completedFuture(null);
                             }
                             return tokenStore.fetchToken(tokenStoreIdentifier, SEGMENT_ID, null)
                                              .handle((token, ex) -> tokenStore.releaseClaim(tokenStoreIdentifier, SEGMENT_ID, null)
                                                                              .thenApply(v -> {
                                                                                  if (ex != null) {
                                                                                      throw ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex);
                                                                                  }
                                                                                  return token;
                                                                              }))
                                              .thenCompose(future -> future);
                         });
    }

    @Override
    public CompletableFuture<Void> storeSafePointToken(@Nonnull TrackingToken token) {
        Objects.requireNonNull(token, "Safe point tracking token must not be null");
        return tokenStore.fetchSegments(tokenStoreIdentifier, null)
                         .thenCompose(segments -> {
                             if (segments.isEmpty()) {
                                 return tokenStore.initializeTokenSegments(tokenStoreIdentifier, 1, token, null)
                                                  .thenApply(v -> null);
                             }
                             return tokenStore.fetchToken(tokenStoreIdentifier, SEGMENT_ID, null)
                                              .thenCompose(v -> tokenStore.storeToken(token, tokenStoreIdentifier, SEGMENT_ID, null))
                                              .handle((v, ex) -> tokenStore.releaseClaim(tokenStoreIdentifier, SEGMENT_ID, null)
                                                                          .thenApply(rv -> {
                                                                              if (ex != null) {
                                                                                  throw ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex);
                                                                              }
                                                                              return (Void) null;
                                                                          }))
                                              .thenCompose(future -> future);
                         });
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("tokenStoreIdentifier", tokenStoreIdentifier);
    }
}
