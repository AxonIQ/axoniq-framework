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
 * Verifies the chain's non-matching path is lazy: the framework's
 * {@code MessageConverter.convertPayload(...)} is never invoked when no transformer
 * matches the event's {@link MessageType}, and lookup completes in constant time
 * per event.
 */
class EventTransformerChainFr011LazyTest {

    private static final MessageType REGISTERED = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType UNREGISTERED = new MessageType("com.example.SystemHeartbeat", "1.0.0");

    @Test
    @Disabled("Tests-first; impl lands in T026 (non-matching path skips converter)")
    void converter_is_never_invoked_when_no_transformer_matches() {
        // given -- mapper-call counter detects converter invocation
        AtomicInteger mapperInvocations = new AtomicInteger();
        EventTransformer t = EventTransformation.from(REGISTERED)
                                                .to(new MessageType("com.example.CourseCreated", "2.0.0"))
                                                .transform(JsonNode.class, (in, ctx) -> {
                                                    mapperInvocations.incrementAndGet();
                                                    return in;
                                                });
        EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

        // and -- 1000 events, none matching
        List<EventMessage> nonMatching = IntStream.range(0, 1000)
                .mapToObj(i -> (EventMessage) new GenericEventMessage(UNREGISTERED, "p-" + i))
                .toList();

        // when
        List<EventMessage> out = drain(chain.transform(MessageStream.fromIterable(nonMatching)));

        // then
        assertThat(out).hasSize(1000);
        assertThat(mapperInvocations.get()).isZero();
    }

    @Test
    @Disabled("Tests-first; impl lands in T026 (concrete-from O(1) QualifiedName-keyed lookup)")
    void non_matching_lookup_completes_in_constant_time_regardless_of_chain_length() {
        // given -- a chain with many registered transformers, each for a different type
        EventTransformerChain.Builder builder = EventTransformerChain.builder();
        for (int i = 0; i < 100; i++) {
            MessageType ft = new MessageType("com.example.Type" + i, "1.0.0");
            MessageType tt = new MessageType("com.example.Type" + i, "2.0.0");
            builder.register(EventTransformation.from(ft).to(tt).transform(JsonNode.class, (in, ctx) -> in));
        }
        EventTransformerChain chain = builder.build();

        // and -- one event matching none of them
        EventMessage heartbeat = new GenericEventMessage(UNREGISTERED, "heartbeat");

        // when
        long start = System.nanoTime();
        List<EventMessage> out = drain(chain.transform(MessageStream.fromIterable(List.of(heartbeat))));
        long elapsed = System.nanoTime() - start;

        // then
        assertThat(out).hasSize(1);
        // not a hard threshold -- the JMH suite (T041) sets concrete numbers; here we verify
        // that 100 registrations don't cause linear-scan blowup (rough: under 10ms is safe)
        assertThat(elapsed).isLessThan(10_000_000L);
    }

    private static List<EventMessage> drain(MessageStream<? extends EventMessage> stream) {
        return stream.<List<EventMessage>>reduce(new ArrayList<>(), (acc, entry) -> {
            acc.add(entry.message());
            return acc;
        }).join();
    }
}
