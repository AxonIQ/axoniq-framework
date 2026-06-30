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

import java.util.Arrays;
import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/**
 * A predicate {@code from} is a fallback: it applies only to events that no exact transformation claims, and when
 * more than one predicate accepts the same event the first registered one wins.
 */
final class EventTransformerChainPredicateFallbackTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V1_PATCH = new MessageType("com.example.CourseCreated", "1.0.1");
    private static final MessageType EXACT_TARGET = new MessageType("com.example.CourseCreated", "2.0.0");
    private static final MessageType PREDICATE_TARGET = new MessageType("com.example.CourseCreated", "3.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    @Test
    void predicateAppliesOnlyToEventsNoExactMatchClaims() {
        // given an exact transformation for V1 and a predicate covering every CourseCreated 1.x
        EventTransformation exact = EventTransformation.from(V1)
                                                       .to(EXACT_TARGET)
                                                       .transform(JsonNode.class, (in, ctx) -> marker("exact"));
        EventTransformation predicate = EventTransformation.from(mt -> mt.version().startsWith("1."))
                                                           .to(PREDICATE_TARGET)
                                                           .transform(JsonNode.class, (in, ctx) -> marker("predicate"));
        EventTransformerChain chain = EventTransformerChain.builder()
                                                           .register(exact)
                                                           .register(predicate)
                                                           .build();

        // when transforming V1 (claimed by the exact match) and V1_PATCH (claimed by neither exactly)
        List<EventMessage> outputs = transform(chain, V1, V1_PATCH);

        // then V1 takes the exact target and V1_PATCH falls back to the predicate target
        assertThat(outputs).hasSize(2);
        assertMarker(outputs.get(0), EXACT_TARGET, "exact");
        assertMarker(outputs.get(1), PREDICATE_TARGET, "predicate");
    }

    @Test
    void firstRegisteredPredicateWinsAmongOverlappingPredicates() {
        // given two predicates that both accept a V1 event
        EventTransformation first = EventTransformation.from(mt -> mt.version().startsWith("1."))
                                                       .to(EXACT_TARGET)
                                                       .transform(JsonNode.class, (in, ctx) -> marker("first"));
        EventTransformation second = EventTransformation.from(mt -> mt.version().startsWith("1.0"))
                                                        .to(PREDICATE_TARGET)
                                                        .transform(JsonNode.class, (in, ctx) -> marker("second"));
        EventTransformerChain chain = EventTransformerChain.builder()
                                                           .register(first)
                                                           .register(second)
                                                           .build();

        // when transforming a V1 event
        List<EventMessage> outputs = transform(chain, V1);

        // then the first registered predicate applies
        assertThat(outputs).hasSize(1);
        assertMarker(outputs.getFirst(), EXACT_TARGET, "first");
    }

    private static ObjectNode marker(String via) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("via", via);
        return payload;
    }

    private static List<EventMessage> transform(EventTransformerChain chain, MessageType... types) {
        List<EventMessage> events = Arrays.stream(types)
                                          .map(type -> (EventMessage) new GenericEventMessage(
                                                  type, JsonNodeFactory.instance.objectNode()))
                                          .toList();
        return collectMessages(chain.transform(MessageStream.fromIterable(events), null, CONVERTER, RESOLVER));
    }

    private static void assertMarker(EventMessage output, MessageType expectedType, String via) {
        assertThat(output.type()).isEqualTo(expectedType);
        assertThat(output.payload())
                .asInstanceOf(type(JsonNode.class))
                .satisfies(node -> assertThat(node.path("via").asText()).isEqualTo(via));
    }
}
