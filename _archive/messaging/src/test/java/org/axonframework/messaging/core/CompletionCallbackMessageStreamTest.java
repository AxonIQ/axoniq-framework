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
import java.util.concurrent.ThreadLocalRandom;

import static org.axonframework.messaging.core.MessageStreamUtils.NO_OP_CALLBACK;

/**
 * Test class validating the {@link CompletionCallbackMessageStream} through the {@link MessageStreamTest} suite.
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 */
class CompletionCallbackMessageStreamTest extends MessageStreamTest<Message> {

    @Override
    protected MessageStream<Message> completedTestSubject(List<Message> messages) {
        return new CompletionCallbackMessageStream<>(MessageStream.fromIterable(messages), NO_OP_CALLBACK);
    }

    @Override
    protected MessageStream.Single<Message> completedSingleStreamTestSubject(Message message) {
        Assumptions.abort("CompletionCallbackMessageStream does not support explicit single-item streams");
        return null;
    }

    @Override
    protected MessageStream.Empty<Message> completedEmptyStreamTestSubject() {
        Assumptions.abort("CompletionCallbackMessageStream does not support explicit zero-item streams");
        return null;
    }

    @Override
    protected MessageStream<Message> failingTestSubject(List<Message> messages,
                                                        RuntimeException failure) {
        return new CompletionCallbackMessageStream<>(MessageStream.fromIterable(messages)
                                                                  .concatWith(MessageStream.failed(failure)),
                                                     NO_OP_CALLBACK);
    }

    @Override
    protected Message createRandomMessage() {
        return new GenericMessage(new MessageType("message"),
                                    "test-" + ThreadLocalRandom.current().nextInt(10000));
    }
}