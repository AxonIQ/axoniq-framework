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
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Acceptance test for {@code EventTransformation.from(...).to(...).transform(...)} -- the
 * 1:1 structural payload transformation that closes the issue's MUST scope (US1).
 * A stored v1 event is observed as v2 by handlers consuming the chain's output stream.
 */
final class StructuralTransformationTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");

    @Test
    void storedV1EventIsObservedAsV2AfterRegisteringV1ToV2Transformation() {
        EventTransformer v1ToV2Transformer = EventTransformation.from(V1)
                                                                .to(V2)
                                                                .transform(JsonNode.class, (v1, ctx) -> {
                                                                    int capacity = v1.get("capacity").asInt();
                                                                    ObjectNode v2 = JsonNodeFactory.instance.objectNode();
                                                                    v2.put("minCapacity", capacity);
                                                                    v2.put("maxCapacity", capacity);
                                                                    return v2;
                                                                });
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();

        ObjectNode v1Payload = JsonNodeFactory.instance.objectNode();
        v1Payload.put("capacity", 30);
        EventMessage storedV1Event = new GenericEventMessage(V1, v1Payload);

        List<EventMessage> observed = collectMessages(chain.transform(MessageStream.fromIterable(List.of(storedV1Event))));

        assertThat(observed).hasSize(1);
        assertThat(observed.getFirst().type()).isEqualTo(V2);
        JsonNode transformedPayload = (JsonNode) observed.getFirst().payload();
        assertThat(transformedPayload.get("minCapacity").asInt()).isEqualTo(30);
        assertThat(transformedPayload.get("maxCapacity").asInt()).isEqualTo(30);
    }

    @Test
    void singleTransformationIsObservedByEveryConsumerOfTheChain() {
        EventTransformer v1ToV2Transformer = EventTransformation.from(V1)
                                                                .to(V2)
                                                                .transform(JsonNode.class, (v1, ctx) -> v1.deepCopy());
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();
        EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        List<EventMessage> firstConsumer = collectMessages(chain.transform(MessageStream.fromIterable(List.of(storedV1Event))));
        List<EventMessage> secondConsumer = collectMessages(chain.transform(MessageStream.fromIterable(List.of(storedV1Event))));

        assertThat(firstConsumer).hasSize(1);
        assertThat(secondConsumer).hasSize(1);
        assertThat(firstConsumer.getFirst().type()).isEqualTo(V2);
        assertThat(secondConsumer.getFirst().type()).isEqualTo(V2);
    }
}
