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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

public class MessageStreamTestUtils {
    private MessageStreamTestUtils() {
        // Prevent instantiation
    }

    /**
     * Asserts that the given {@code stream} completed exceptionally with the given {@code expectedExceptionType}
     * @param stream The {@link MessageStream} to assert.
     * @param expectedExceptionType The expected type of the exception.
     */
    public static void assertCompletedExceptionally(
            MessageStream<?> stream,
            Class<? extends Throwable> expectedExceptionType
    ) {
        CompletableFuture<? extends MessageStream.Entry<?>> cf = stream.first().asCompletableFuture();
        await().atMost(10, TimeUnit.SECONDS)
               .until(cf::isDone);
        var exception = cf.exceptionNow();
        assertInstanceOf(expectedExceptionType, exception);
    }

    /**
     * Asserts that the given {@code stream} completed exceptionally with the given {@code expectedExceptionType} and
     * {@code expectedMessage}.
     * @param stream The {@link MessageStream} to assert.
     * @param expectedExceptionType The expected type of the exception.
     * @param expectedMessagePart Part of the expected message of the exception.
     */
    public static void assertCompletedExceptionally(
            MessageStream<?> stream,
            Class<? extends Throwable> expectedExceptionType,
            String expectedMessagePart
    ) {
        CompletableFuture<? extends MessageStream.Entry<?>> cf = stream.first().asCompletableFuture();
        var exception = assertThrows(CompletionException.class, cf::join);
        assertInstanceOf(expectedExceptionType, exception.getCause());
        assertTrue(exception.getCause().getMessage().contains(expectedMessagePart),
                   "Expected message to contain [%s],\n but was [%s]".formatted(expectedMessagePart,
                                                                                exception.getCause().getMessage()));
    }
}
