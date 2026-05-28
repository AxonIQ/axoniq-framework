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

import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Token store test.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class TokenStoreSafePointStoreTest {

    @Test
    void fetchReturnsNullWhenNothingStoredYet() {
        var store = new TokenStoreSafePointStore(
                new InMemoryTokenStore(),
                TokenStoreSafePointStore.tokenStoreIdentifier("Workflow")
        );

        assertThat(store.fetchSafePointToken().join()).isNull();
    }

    @Test
    void storeAndFetchRoundTripSafePointToken() {
        var store = new TokenStoreSafePointStore(
                new InMemoryTokenStore(),
                TokenStoreSafePointStore.tokenStoreIdentifier("Workflow")
        );
        TrackingToken expected = new GlobalSequenceTrackingToken(18);

        store.storeSafePointToken(expected).join();

        assertSameToken(store.fetchSafePointToken().join(), expected);
    }

    private static void assertSameToken(TrackingToken actual, TrackingToken expected) {
        assertThat(actual.lowerBound(expected)).isEqualTo(expected);
        assertThat(expected.lowerBound(actual)).isEqualTo(actual);
    }
}
