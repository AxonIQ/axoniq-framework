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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Acceptance test for {@code EventTransformation.from(...).to(...).transform(...)} —
 * the 1:1 structural payload transformation that closes the issue's MUST scope (US1).
 * Covers scenarios 1 + 2: a stored v1 event is observed as v2 by handlers consuming the
 * chain's output stream.
 */
class EventTransformationFr001Test {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");

    @Test
    @Disabled("Tests-first; impl lands in T023 (EventTransformation factory) + T024 (transform overloads) + T027 (chain matching)")
    void stored_v1_event_is_observed_as_v2_after_registering_a_v1_to_v2_transformation() {
        // given
        EventTransformer t = EventTransformation.from(V1)
                                                .to(V2)
                                                .transform(JsonNode.class, (v1, ctx) -> {
                                                    int capacity = v1.get("capacity").asInt();
                                                    ObjectNode v2 = JsonNodeFactory.instance.objectNode();
                                                    v2.put("minCapacity", capacity);
                                                    v2.put("maxCapacity", capacity);
                                                    return v2;
                                                });
        EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

        ObjectNode v1Payload = JsonNodeFactory.instance.objectNode();
        v1Payload.put("capacity", 30);
        EventMessage stored = new GenericEventMessage(V1, v1Payload);

        // when
        List<EventMessage> observed = drain(chain.transform(MessageStream.fromIterable(List.of(stored))));

        // then -- consumer observes v2
        assertThat(observed).hasSize(1);
        assertThat(observed.get(0).type()).isEqualTo(V2);
        JsonNode out = (JsonNode) observed.get(0).payload();
        assertThat(out.get("minCapacity").asInt()).isEqualTo(30);
        assertThat(out.get("maxCapacity").asInt()).isEqualTo(30);
    }

    @Test
    @Disabled("Tests-first; impl lands in T023 / T027")
    void a_single_transformation_is_observed_by_every_consumer_of_the_chain() {
        // given -- same chain consumed twice (mimics two handlers reading the same stream)
        EventTransformer t = EventTransformation.from(V1)
                                                .to(V2)
                                                .transform(JsonNode.class, (v1, ctx) -> v1.deepCopy());
        EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

        EventMessage stored = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        // when
        List<EventMessage> observerA = drain(chain.transform(MessageStream.fromIterable(List.of(stored))));
        List<EventMessage> observerB = drain(chain.transform(MessageStream.fromIterable(List.of(stored))));

        // then -- both consumers see identical transformed event
        assertThat(observerA).hasSize(1);
        assertThat(observerB).hasSize(1);
        assertThat(observerA.get(0).type()).isEqualTo(V2);
        assertThat(observerB.get(0).type()).isEqualTo(V2);
    }

    private static List<EventMessage> drain(MessageStream<? extends EventMessage> stream) {
        return stream.<List<EventMessage>>reduce(new ArrayList<>(), (acc, entry) -> {
            acc.add(entry.message());
            return acc;
        }).join();
    }
}
