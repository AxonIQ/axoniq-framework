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

package org.axonframework.messaging.core;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link HandlerExecutionException}.
 */
class HandlerExecutionExceptionTest {

    @Test
    void resolveDetailsFromNestedExecutionException() {
        Exception exception = new RuntimeException(new StubHandlerExecutionException("test", null, "Details!"));

        assertEquals("Details!", HandlerExecutionException.resolveDetails(exception).orElse(null));
    }

    @Test
    void resolveDetailsFromExecutionException() {
        Exception exception = new StubHandlerExecutionException("test", null, "Details!");

        assertEquals("Details!", HandlerExecutionException.resolveDetails(exception).orElse(null));
    }

    @Test
    void resolveDetailsFromNull() {
        assertFalse(HandlerExecutionException.resolveDetails(null).isPresent());
    }

    @Test
    void resolveDetailsFromRuntimeException() {
        assertFalse(HandlerExecutionException.resolveDetails(new RuntimeException()).isPresent());
    }

    @Test
    void validatePresenceOfStackTraceWithWritableStackTraceSetting() {
        Exception exception = new StubHandlerExecutionException("Some message");
        assertEquals(0, exception.getStackTrace().length);

        exception = new StubHandlerExecutionException("Some message", new RuntimeException());
        assertEquals(0, exception.getStackTrace().length);

        exception = new StubHandlerExecutionException("Some message", new RuntimeException(), "Some details");
        assertEquals(0, exception.getStackTrace().length);

        exception = new StubHandlerExecutionException("Some message", new RuntimeException(), "Some details", false);
        assertEquals(0, exception.getStackTrace().length);

        exception = new StubHandlerExecutionException("Some message", new RuntimeException(), "Some details", true);
        assertTrue(exception.getStackTrace().length > 0);
    }

    private static class StubHandlerExecutionException extends HandlerExecutionException {

        public StubHandlerExecutionException(String message) {
            super(message);
        }

        public StubHandlerExecutionException(String message, Throwable cause) {
            super(message, cause);
        }

        public StubHandlerExecutionException(String message, Throwable cause, Object details) {
            super(message, cause, details);
        }

        public StubHandlerExecutionException(String message,
                                             Throwable cause,
                                             Object details,
                                             boolean writableStackTrace) {
            super(message, cause, details, writableStackTrace);
        }
    }
}