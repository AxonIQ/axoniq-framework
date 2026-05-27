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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The chain's non-matching path is lazy: the framework's
 * {@code MessageConverter.convertPayload(...)} is never invoked when no transformer
 * matches the event's {@link MessageType}, and lookup completes in constant time
 * per event.
 */
final class LazyDeserializationTest {

    private static final MessageType REGISTERED = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType UNREGISTERED = new MessageType("com.example.SystemHeartbeat", "1.0.0");

    @Test
    @Disabled("Tests-first; impl lands in T026 (non-matching path skips converter)")
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
                .mapToObj(i -> (EventMessage) new GenericEventMessage(UNREGISTERED, "p-" + i))
                .toList();

        List<EventMessage> outputs = drain(chain.transform(MessageStream.fromIterable(nonMatchingEvents)));

        assertThat(outputs).hasSize(1000);
        assertThat(mapperInvocations.get()).isZero();
    }

    @Test
    @Disabled("Tests-first; impl lands in T026 (concrete-from O(1) QualifiedName-keyed lookup)")
    void nonMatchingLookupCompletesInConstantTimeRegardlessOfChainLength() {
        EventTransformerChain.Builder builder = EventTransformerChain.builder();
        for (int i = 0; i < 100; i++) {
            MessageType fromType = new MessageType("com.example.Type" + i, "1.0.0");
            MessageType toType = new MessageType("com.example.Type" + i, "2.0.0");
            builder.register(EventTransformation.from(fromType).to(toType).transform(JsonNode.class, (in, ctx) -> in));
        }
        EventTransformerChain chain = builder.build();
        EventMessage unregisteredEvent = new GenericEventMessage(UNREGISTERED, "heartbeat");

        long startedAt = System.nanoTime();
        List<EventMessage> outputs = drain(chain.transform(MessageStream.fromIterable(List.of(unregisteredEvent))));
        long elapsedNanos = System.nanoTime() - startedAt;

        assertThat(outputs).hasSize(1);
        assertThat(elapsedNanos).isLessThan(10_000_000L);
    }

    private static List<EventMessage> drain(MessageStream<? extends EventMessage> stream) {
        List<EventMessage> collected = new ArrayList<>();
        stream.<Void>reduce(null, (acc, entry) -> {
            collected.add(entry.message());
            return null;
        }).join();
        return collected;
    }
}
