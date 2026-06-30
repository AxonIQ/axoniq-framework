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
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/**
 * An exact {@code from} match always takes precedence over a predicate match that also accepts the event, regardless
 * of the order the two were registered. The predicate only ever applies to events no exact match claims.
 */
final class EventTransformerChainExactMatchPrecedenceTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");
    private static final MessageType V3 = new MessageType("com.example.CourseCreated", "3.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    @Test
    void exactMatchWinsWhenRegisteredAfterAnOverlappingPredicate() {
        // given a predicate covering 1.x registered before an exact V1 transformation
        EventTransformerChain chain = EventTransformerChain.builder()
                                                           .register(predicateOneXToV3())
                                                           .register(exactV1ToV2())
                                                           .build();

        // when a V1 event is transformed
        List<EventMessage> outputs = transformSingle(chain, V1);

        // then the exact transformation wins
        assertTransformedTo(outputs, V2, "exact");
    }

    @Test
    void exactMatchWinsWhenRegisteredBeforeAnOverlappingPredicate() {
        // given an exact V1 transformation registered before a predicate covering 1.x
        EventTransformerChain chain = EventTransformerChain.builder()
                                                           .register(exactV1ToV2())
                                                           .register(predicateOneXToV3())
                                                           .build();

        // when a V1 event is transformed
        List<EventMessage> outputs = transformSingle(chain, V1);

        // then the exact transformation still wins, independent of registration order
        assertTransformedTo(outputs, V2, "exact");
    }

    private static EventTransformation exactV1ToV2() {
        return EventTransformation.from(V1).to(V2).transform(JsonNode.class, (in, ctx) -> marker("exact"));
    }

    private static EventTransformation predicateOneXToV3() {
        return EventTransformation.from(mt -> mt.version().startsWith("1."))
                                  .to(V3)
                                  .transform(JsonNode.class, (in, ctx) -> marker("predicate"));
    }

    private static ObjectNode marker(String via) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("via", via);
        return payload;
    }

    private static List<EventMessage> transformSingle(EventTransformerChain chain, MessageType type) {
        EventMessage event = new GenericEventMessage(type, JsonNodeFactory.instance.objectNode());
        return collectMessages(chain.transform(MessageStream.fromIterable(List.of(event)), null, CONVERTER, RESOLVER));
    }

    private static void assertTransformedTo(List<EventMessage> outputs, MessageType expectedType, String via) {
        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst().type()).isEqualTo(expectedType);
        assertThat(outputs.getFirst().payload())
                .asInstanceOf(type(JsonNode.class))
                .satisfies(node -> assertThat(node.path("via").asText()).isEqualTo(via));
    }
}
