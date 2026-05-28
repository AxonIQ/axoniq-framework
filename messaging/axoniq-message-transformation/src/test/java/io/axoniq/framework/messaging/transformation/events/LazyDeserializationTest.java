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

package io.axoniq.framework.messaging.transformation.events;

import com.fasterxml.jackson.databind.JsonNode;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The chain's non-matching path is lazy: the framework's
 * {@code MessageConverter.convertPayload(...)} is never invoked when no transformer
 * matches the event's {@link MessageType}, and events pass through unchanged regardless
 * of chain length. (Wall-clock complexity is verified by JMH, not here.)
 */
final class LazyDeserializationTest {

    private static final MessageType REGISTERED = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType UNREGISTERED = new MessageType("com.example.SystemHeartbeat", "1.0.0");

    @Test
    void converterIsNeverInvokedWhenNoTransformerMatches() {
        AtomicInteger mapperInvocations = new AtomicInteger();
        EventTransformer registeredTransformer = EventTransformation.from(REGISTERED)
                                                                    .to(new MessageType("com.example.CourseCreated", "2.0.0"))
                                                                    .transform(JsonNode.class, (in, ctx) -> {
                                                                        mapperInvocations.incrementAndGet();
                                                                        return in;
                                                                    });
        EventTransformerChain chain = EventTransformerChain.builder().register(registeredTransformer).build();

        List<EventMessage> nonMatchingEvents = IntStream.range(0, 1000)
                .mapToObj(index -> (EventMessage) new GenericEventMessage(UNREGISTERED, "p-" + index))
                .toList();

        List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(nonMatchingEvents)));

        assertThat(outputs).hasSize(1000);
        assertThat(mapperInvocations.get()).isZero();
    }

    @Test
    void nonMatchingLookupReturnsInputUnchangedRegardlessOfChainLength() {
        EventTransformerChain.Builder builder = EventTransformerChain.builder();
        for (int index = 0; index < 100; index++) {
            MessageType fromType = new MessageType("com.example.Type" + index, "1.0.0");
            MessageType toType = new MessageType("com.example.Type" + index, "2.0.0");
            builder.register(EventTransformation.from(fromType).to(toType).transform(JsonNode.class, (in, ctx) -> in));
        }
        EventTransformerChain chain = builder.build();
        EventMessage unregisteredEvent = new GenericEventMessage(UNREGISTERED, "heartbeat");

        List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(List.of(unregisteredEvent))));

        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst()).isSameAs(unregisteredEvent);
    }
}
