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
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Both {@code transform(...)} overloads: {@code transform(Class<T>, BiFunction)} for
 * non-generic input types and {@code transform(TypeReference<T>, BiFunction)} for generic
 * input types such as {@code Map<String, Object>}.
 */
final class TypedPayloadAccessTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");

    @Nested
    final class ClassOverload {

        @Test
        @Disabled("Tests-first; impl lands in T024 (Class<T> overload) + T027 (chain invokes converter)")
        void convertsPayloadToJsonNodeBeforeInvokingMapper() {
            EventTransformer v1ToV2Transformer = EventTransformation.from(V1)
                                                                    .to(V2)
                                                                    .transform(JsonNode.class, (jsonNode, ctx) -> {
                                                                        ObjectNode v2 = JsonNodeFactory.instance.objectNode();
                                                                        v2.put("name", jsonNode.get("name").asText());
                                                                        return v2;
                                                                    });
            EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();

            ObjectNode v1Payload = JsonNodeFactory.instance.objectNode();
            v1Payload.put("name", "Math 101");
            EventMessage storedV1Event = new GenericEventMessage(V1, v1Payload);

            List<EventMessage> outputs = drain(chain.transform(MessageStream.fromIterable(List.of(storedV1Event))));

            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
            JsonNode transformedPayload = (JsonNode) outputs.getFirst().payload();
            assertThat(transformedPayload.get("name").asText()).isEqualTo("Math 101");
        }
    }

    @Nested
    final class TypeReferenceOverload {

        @Test
        @Disabled("Tests-first; impl lands in T024 (TypeReference<T> overload)")
        void preservesGenericTypeSoLambdaParameterTypeIsInferredAtCompileTime() {
            TypeReference<Map<String, Object>> mapType = new TypeReference<>() {
            };
            EventTransformer v1ToV2Transformer = EventTransformation.from(V1)
                                                                    .to(V2)
                                                                    .transform(mapType, (payload, ctx) -> {
                                                                        Map<String, Object> result = new HashMap<>(payload);
                                                                        result.put("upgraded", true);
                                                                        return result;
                                                                    });
            EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();

            Map<String, Object> v1Payload = new HashMap<>();
            v1Payload.put("name", "Math 101");
            EventMessage storedV1Event = new GenericEventMessage(V1, v1Payload);

            List<EventMessage> outputs = drain(chain.transform(MessageStream.fromIterable(List.of(storedV1Event))));

            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
            @SuppressWarnings("unchecked")
            Map<String, Object> transformedPayload = (Map<String, Object>) outputs.getFirst().payload();
            assertThat(transformedPayload).containsEntry("name", "Math 101")
                                          .containsEntry("upgraded", true);
        }
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
