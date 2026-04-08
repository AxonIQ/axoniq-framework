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

package org.axonframework.messaging.commandhandling;

import org.axonframework.common.ExceptionUtils;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Stefan Andjelkovic
 */
class ExceptionUtilsTest {
    @Test
    void isExplicitlyNonTransientForTransientExceptions() {
        NoHandlerForCommandException transientException = new NoHandlerForCommandException("No handler message");
        Assertions.assertFalse(ExceptionUtils.isExplicitlyNonTransient(transientException));
    }

    @Test
    void isExplicitlyNonTransientForRegularExceptions() {
        RuntimeException runtimeException = new RuntimeException("Something went wrong");
        assertFalse(ExceptionUtils.isExplicitlyNonTransient(runtimeException));
    }
}