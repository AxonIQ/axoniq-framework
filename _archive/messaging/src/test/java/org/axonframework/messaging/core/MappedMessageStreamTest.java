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

import org.axonframework.messaging.core.MessageStream.Entry;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * Test class validating the {@link MappedMessageStream} through the {@link MessageStreamTest} suite.
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 */
class MappedMessageStreamTest extends MessageStreamTest<Message> {

    private static final Function<Entry<Message>, Entry<Message>> NO_OP_MAPPER = entry -> entry;

    @Override
    protected MessageStream<Message> completedTestSubject(List<Message> messages) {
        if (messages.size() == 1) {
            return new MappedMessageStream.Single<>(MessageStream.just(messages.getFirst()), NO_OP_MAPPER);
        }
        return new MappedMessageStream<>(MessageStream.fromIterable(messages), NO_OP_MAPPER);
    }

    @Override
    protected MessageStream.Single<Message> completedSingleStreamTestSubject(Message message) {
        return new MappedMessageStream.Single<>(MessageStream.just(message), NO_OP_MAPPER);
    }

    @Override
    protected MessageStream.Empty<Message> completedEmptyStreamTestSubject() {
        Assumptions.abort("MappedMessageStream does not support empty streams");
        return null;
    }

    @Override
    protected MessageStream<Message> failingTestSubject(List<Message> entries, RuntimeException failure) {
        return new MappedMessageStream<>(MessageStream.fromIterable(entries)
                                                      .concatWith(MessageStream.failed(failure)), NO_OP_MAPPER);
    }

    @Override
    protected Message createRandomMessage() {
        return new GenericMessage(new MessageType("message"),
                                    "test-" + ThreadLocalRandom.current().nextInt(10000));
    }
}