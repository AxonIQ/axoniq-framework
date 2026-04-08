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

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link FailedMessageStream} through the {@link MessageStreamTest} suite.
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 */
class FailedMessageStreamTest extends MessageStreamTest<Message> {

    @Override
    protected MessageStream<Message> completedTestSubject(List<Message> messages) {
        Assumptions.abort("FailedMessageStream doesn't support successful streams");
        return null;
    }

    @Override
    protected MessageStream.Single<Message> completedSingleStreamTestSubject(Message message) {
        Assumptions.abort("FailedMessageStream doesn't support successful streams");
        return null;
    }

    @Override
    protected MessageStream.Empty<Message> completedEmptyStreamTestSubject() {
        Assumptions.abort("FailedMessageStream doesn't support successful streams");
        return null;
    }

    @Override
    protected MessageStream<Message> failingTestSubject(List<Message> messages,
                                                        RuntimeException failure) {
        Assumptions.assumeTrue(messages.isEmpty(), "FailedMessageStream doesn't support content");
        return MessageStream.failed(failure);
    }

    @Override
    protected Message createRandomMessage() {
        Assumptions.abort("FailedMessageStream doesn't support content");
        return null;
    }

    @Test
    void shouldPropagateErrorToCompletableFuture() {
        var testSubject = new FailedMessageStream<>(new RuntimeException("Expected"));

        var future = testSubject.asCompletableFuture();

        assertTrue(future.isCompletedExceptionally());
    }
}