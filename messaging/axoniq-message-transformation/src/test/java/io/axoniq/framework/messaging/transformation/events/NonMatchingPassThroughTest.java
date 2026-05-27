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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that events whose {@link MessageType} matches no registered transformation pass
 * through the chain unchanged, and that no payload conversion happens on the non-matching
 * path.
 */
class NonMatchingPassThroughTest {

    private static final MessageType REGISTERED = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType UNREGISTERED = new MessageType("com.example.SystemHeartbeat", "1.0.0");

    @Test
    @Disabled("Tests-first; impl lands in T023 + T026 (non-matching pass-through path)")
    void event_whose_type_matches_no_registered_transformer_passes_through_unchanged() {
        // given -- only one transformation registered, for REGISTERED type
        EventTransformer t = EventTransformation.from(REGISTERED)
                                                .to(new MessageType("com.example.CourseCreated", "2.0.0"))
                                                .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

        // and -- a stream containing an UNREGISTERED-typed event
        EventMessage heartbeat = new GenericEventMessage(UNREGISTERED, "heartbeat-payload");

        // when
        List<EventMessage> out = drain(chain.transform(MessageStream.fromIterable(List.of(heartbeat))));

        // then -- unchanged
        assertThat(out).hasSize(1);
        assertThat(out.get(0)).isSameAs(heartbeat);
        assertThat(out.get(0).type()).isEqualTo(UNREGISTERED);
    }

    @Test
    @Disabled("Tests-first; impl lands in T026 (FR-011 lazy: no converter invocation on non-matching)")
    void no_payload_conversion_happens_for_non_matching_events() {
        // given -- counter wraps the converter to detect invocations
        AtomicInteger conversionCount = new AtomicInteger();
        // (the chain would call MessageConverter.convertPayload(...) on matching events;
        //  for non-matching, this counter must remain zero.)
        EventTransformer t = EventTransformation.from(REGISTERED)
                                                .to(new MessageType("com.example.CourseCreated", "2.0.0"))
                                                .transform(JsonNode.class, (in, ctx) -> {
                                                    conversionCount.incrementAndGet();
                                                    return in;
                                                });
        EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

        EventMessage heartbeat = new GenericEventMessage(UNREGISTERED, "heartbeat-payload");

        // when
        drain(chain.transform(MessageStream.fromIterable(List.of(heartbeat))));

        // then -- mapper never invoked
        assertThat(conversionCount.get()).isZero();
    }

    private static List<EventMessage> drain(MessageStream<? extends EventMessage> stream) {
        return stream.<List<EventMessage>>reduce(new ArrayList<>(), (acc, entry) -> {
            acc.add(entry.message());
            return acc;
        }).join();
    }
}
