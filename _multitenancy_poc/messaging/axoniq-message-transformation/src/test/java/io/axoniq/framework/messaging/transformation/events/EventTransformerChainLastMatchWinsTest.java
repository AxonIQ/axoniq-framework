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
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
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
 * When more than one registered transformation would match a given event, the chain applies
 * the LAST registration that matches. Reads as: later registrations override earlier
 * overlapping ones. Single-hop only; multi-hop iteration (v1 -> v2 -> v3 chained) is
 * verified separately.
 */
final class EventTransformerChainLastMatchWinsTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");
    private static final MessageType V3 = new MessageType("com.example.CourseCreated", "3.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    @Test
    void laterExactRegistrationOverridesEarlierPredicateRegistrationOnOverlappingMatch() {
        EventTransformation earlierPredicateToV3 = EventTransformation.from(mt -> mt.version().startsWith("1."))
                                                                    .to(V3)
                                                                    .transform(JsonNode.class, (in, ctx) -> {
                                                                        ObjectNode out = JsonNodeFactory.instance.objectNode();
                                                                        out.put("via", "predicate");
                                                                        return out;
                                                                    });
        EventTransformation laterExactToV2 = EventTransformation.from(V1)
                                                                 .to(V2)
                                                                 .transform(JsonNode.class, (in, ctx) -> {
                                                                     ObjectNode out = JsonNodeFactory.instance.objectNode();
                                                                     out.put("via", "exact");
                                                                     return out;
                                                                 });
        EventTransformerChain chain = EventTransformerChain.builder()
                                                            .register(earlierPredicateToV3)
                                                            .register(laterExactToV2)
                                                            .build();

        EventMessage v1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(List.of(v1Event)), null, CONVERTER, RESOLVER));

        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst().type()).isEqualTo(V2);
        assertThat(outputs.getFirst().payload())
                .asInstanceOf(type(JsonNode.class))
                .satisfies(node -> assertThat(node.path("via").asText()).isEqualTo("exact"));
    }

    @Test
    void laterPredicateRegistrationOverridesEarlierExactRegistrationOnOverlappingMatch() {
        EventTransformation earlierExactToV2 = EventTransformation.from(V1)
                                                                  .to(V2)
                                                                  .transform(JsonNode.class, (in, ctx) -> {
                                                                      ObjectNode out = JsonNodeFactory.instance.objectNode();
                                                                      out.put("via", "exact");
                                                                      return out;
                                                                  });
        EventTransformation laterPredicateToV3 = EventTransformation.from(mt -> mt.version().startsWith("1."))
                                                                  .to(V3)
                                                                  .transform(JsonNode.class, (in, ctx) -> {
                                                                      ObjectNode out = JsonNodeFactory.instance.objectNode();
                                                                      out.put("via", "predicate");
                                                                      return out;
                                                                  });
        EventTransformerChain chain = EventTransformerChain.builder()
                                                           .register(earlierExactToV2)
                                                           .register(laterPredicateToV3)
                                                           .build();

        EventMessage v1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(List.of(v1Event)), null, CONVERTER, RESOLVER));

        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst().type()).isEqualTo(V3);
        assertThat(outputs.getFirst().payload())
                .asInstanceOf(type(JsonNode.class))
                .satisfies(node -> assertThat(node.path("via").asText()).isEqualTo("predicate"));
    }
}
