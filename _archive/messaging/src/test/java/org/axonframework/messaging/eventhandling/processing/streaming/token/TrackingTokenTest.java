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

package org.axonframework.messaging.eventhandling.processing.streaming.token;

import org.axonframework.messaging.core.Context;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@code static} methods from the {@link TrackingToken} interface.
 *
 * @author Steven van Beelen
 */
class TrackingTokenTest {

    @Test
    void addToContextAddsTheGivenTrackingTokenToTheGivenContext() {
        Context testContext = Context.empty();
        TestToken testToken = new TestToken();

        testContext = TrackingToken.addToContext(testContext, testToken);

        assertTrue(testContext.containsResource(TrackingToken.RESOURCE_KEY));
    }

    @Test
    void fromContextReturnsAnEmptyOptionalWhenNoTokenIsPresent() {
        Context testContext = Context.empty();

        Optional<TrackingToken> result = TrackingToken.fromContext(testContext);

        assertTrue(result.isEmpty());
    }

    @Test
    void fromContextReturnsAnOptionalWithTheContainedToken() {
        Context testContext = Context.empty();
        TestToken testToken = new TestToken();

        testContext = TrackingToken.addToContext(testContext, testToken);

        Optional<TrackingToken> result = TrackingToken.fromContext(testContext);

        assertFalse(result.isEmpty());
        assertEquals(testToken, result.get());
    }

    private static class TestToken implements TrackingToken {

        @Override
        public TrackingToken lowerBound(TrackingToken other) {
            throw new UnsupportedOperationException("Not needed for testing");
        }

        @Override
        public TrackingToken upperBound(TrackingToken other) {
            throw new UnsupportedOperationException("Not needed for testing");
        }

        @Override
        public boolean covers(TrackingToken other) {
            throw new UnsupportedOperationException("Not needed for testing");
        }
    }
}