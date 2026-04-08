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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;

/**
 * Test class validating the default implemented methods of the {@link org.axonframework.messaging.eventhandling.EventMessageHandler}.
 *
 * @author Steven van Beelen
 */
class EventMessageHandlerTest {

    @SuppressWarnings("Convert2Lambda") // Cannot spy a lambda
    private final org.axonframework.messaging.eventhandling.EventMessageHandler testSubject = spy(new EventMessageHandler() {
        @Override
        public @NonNull Object handleSync(@NonNull EventMessage event, @NonNull ProcessingContext context) throws Exception {
            return null;
        }
    });

    @Test
    void prepareResetWithNullResetContextInvokesPrepareReset() {
        testSubject.prepareReset(null, null);

        verify(testSubject).prepareReset(null);
    }

    @Test
    void prepareResetWithNonNullThrowsUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () -> testSubject.prepareReset("non-null", null));
    }
}
