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

import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * When more than one registered transformer would match a given event, the chain applies
 * the LAST registration that matches. Reads as: later registrations override earlier
 * overlapping ones. Single-hop only -- multi-hop iteration (v1 -> v2 -> v3 chained) is
 * verified separately by deferred multi-hop hardening tests.
 */
final class LastMatchWinsTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");
    private static final MessageType V3 = new MessageType("com.example.CourseCreated", "3.0.0");

    @Test
    @Disabled("Tests-first; impl lands in T027 (chain fixed-point iteration with last-match-wins)")
    void laterConcreteRegistrationOverridesEarlierPredicateRegistrationOnOverlappingMatch() {
        EventTransformer earlierPredicateToV3 = EventTransformation.from(mt -> mt.version().startsWith("1."))
                                                                    .to(V3)
                                                                    .transform(JsonNode.class, (in, ctx) -> {
                                                                        ObjectNode out = JsonNodeFactory.instance.objectNode();
                                                                        out.put("via", "predicate");
                                                                        return out;
                                                                    });
        EventTransformer laterConcreteToV2 = EventTransformation.from(V1)
                                                                 .to(V2)
                                                                 .transform(JsonNode.class, (in, ctx) -> {
                                                                     ObjectNode out = JsonNodeFactory.instance.objectNode();
                                                                     out.put("via", "concrete");
                                                                     return out;
                                                                 });
        EventTransformerChain chain = EventTransformerChain.builder()
                                                            .register(earlierPredicateToV3)
                                                            .register(laterConcreteToV2)
                                                            .build();

        EventMessage v1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(List.of(v1Event))));

        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst().type()).isEqualTo(V2);
        JsonNode transformedPayload = (JsonNode) outputs.getFirst().payload();
        assertThat(transformedPayload.get("via").asText()).isEqualTo("concrete");
    }
}
