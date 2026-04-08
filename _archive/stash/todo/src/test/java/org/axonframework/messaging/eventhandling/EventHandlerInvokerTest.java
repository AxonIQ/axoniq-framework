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

package org.axonframework.messaging.eventhandling;

import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import org.jspecify.annotations.NonNull;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the default implemented methods of the {@link org.axonframework.messaging.eventhandling.EventHandlerInvoker}.
 *
 * @author Steven van Beelen
 */
class EventHandlerInvokerTest {

    private final org.axonframework.messaging.eventhandling.EventHandlerInvoker testSubject = spy(new EventHandlerInvoker() {
        @Override
        public boolean canHandle(@NonNull EventMessage eventMessage, @NonNull ProcessingContext context, @NonNull Segment segment) {
            return true;
        }

        @Override
        public void handle(@NonNull EventMessage message, @NonNull ProcessingContext processingContext,
                           @NonNull Segment segment) throws Exception {
            // Do nothing
        }
    });

    @Test
    void performResetWithNullResetContextInvokesPerformReset() {
        testSubject.performReset(null, null);

        verify(testSubject).performReset(null);
    }

    @Test
    void performResetWithNonNullThrowsUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () -> testSubject.performReset("non-null", null));
    }
}
