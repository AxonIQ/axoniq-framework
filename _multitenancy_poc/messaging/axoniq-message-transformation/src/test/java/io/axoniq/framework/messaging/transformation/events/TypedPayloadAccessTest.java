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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.recordingConverter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/**
 * Both {@code transform(...)} overloads: {@code transform(Class<T>, BiFunction)} for
 * non-generic input types and {@code transform(TypeReference<T>, BiFunction)} for generic
 * input types such as {@code Map<String, Object>}.
 */
final class TypedPayloadAccessTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    @Nested
    final class ClassOverload {

        @Test
        void fastPathSkipsConverterWhenPayloadIsAlreadyOfDeclaredInputClass() {
            EventTransformer v1ToV2Transformer = EventTransformer.from(V1)
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

            List<EventMessage> outputs = collectMessages(
                    chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER, RESOLVER));

            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
            assertThat(outputs.getFirst().payload())
                    .asInstanceOf(type(JsonNode.class))
                    .satisfies(node -> assertThat(node.get("name").asText()).isEqualTo("Math 101"));
        }

        @Test
        void slowPathInvokesConverterWithDeclaredInputClassWhenStoredPayloadIsADifferentType() {
            // The stored payload is a raw JSON String; the transformer declares JsonNode.class.
            // The framework must invoke MessageConverter.convertPayload(message, JsonNode.class)
            // before invoking the mapper.
            EventTransformer v1ToV2Transformer = EventTransformer.from(V1)
                                                                    .to(V2)
                                                                    .transform(JsonNode.class, (jsonNode, ctx) -> {
                                                                        ObjectNode v2 = JsonNodeFactory.instance.objectNode();
                                                                        v2.put("name", jsonNode.get("name").asText());
                                                                        return v2;
                                                                    });
            EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();

            EventMessage storedV1Event = new GenericEventMessage(V1, "{\"name\":\"Math 101\"}");
            EventStreamTestUtils.RecordingMessageConverter<JsonNode> recording =
                    recordingConverter(message -> parseJson((String) message.payload()));

            List<EventMessage> outputs = collectMessages(
                    chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, recording, RESOLVER));

            assertThat(recording.invocationCount()).isEqualTo(1);
            assertThat(recording.lastRequestedType()).isEqualTo(JsonNode.class);
            assertThat(recording.lastRequestedMessage()).isSameAs(storedV1Event);
            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
            assertThat(outputs.getFirst().payload())
                    .asInstanceOf(type(JsonNode.class))
                    .satisfies(node -> assertThat(node.get("name").asText()).isEqualTo("Math 101"));
        }
    }

    @Nested
    final class TypeReferenceOverload {

        @Test
        void fastPathSkipsConverterWhenPayloadIsAlreadyAMap() {
            TypeReference<Map<String, Object>> mapType = new TypeReference<>() {
            };
            EventTransformer v1ToV2Transformer = EventTransformer.from(V1)
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

            List<EventMessage> outputs = collectMessages(
                    chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER, RESOLVER));

            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
            assertThat(outputs.getFirst().payload())
                    .asInstanceOf(MAP)
                    .containsEntry("name", "Math 101")
                    .containsEntry("upgraded", true);
        }

        @Test
        void slowPathInvokesConverterWithParameterizedTypeWhenStoredPayloadIsADifferentType() {
            // Stored payload is a raw JSON String; the transformer declares TypeReference<Map<String, Object>> --
            // the framework must invoke MessageConverter.convertPayload(message, parameterizedMapType) so
            // the generic parameters survive (TypeReference preserves the parameterized type at runtime).
            TypeReference<Map<String, Object>> mapType = new TypeReference<>() {
            };
            EventTransformer v1ToV2Transformer = EventTransformer.from(V1)
                                                                    .to(V2)
                                                                    .transform(mapType, (payload, ctx) -> {
                                                                        Map<String, Object> result = new HashMap<>(payload);
                                                                        result.put("upgraded", true);
                                                                        return result;
                                                                    });
            EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();

            EventMessage storedV1Event = new GenericEventMessage(V1, "{\"name\":\"Math 101\"}");
            EventStreamTestUtils.RecordingMessageConverter<Map<String, Object>> recording =
                    recordingConverter(message -> parseJsonToMap((String) message.payload()));

            List<EventMessage> outputs = collectMessages(
                    chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, recording, RESOLVER));

            assertThat(recording.invocationCount()).isEqualTo(1);
            assertThat(recording.lastRequestedType())
                    .as("framework must pass the parameterized Map<String, Object> type, not the raw class")
                    .isInstanceOf(ParameterizedType.class)
                    .isEqualTo(mapType.getType());
            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
            assertThat(outputs.getFirst().payload())
                    .asInstanceOf(MAP)
                    .containsEntry("name", "Math 101")
                    .containsEntry("upgraded", true);
        }
    }

    @Nested
    final class ContextFreeOverload {

        @Test
        void classOverloadMapsWithoutAProcessingContextAndStillInvokesTheConverter() {
            // given a transformation registered with a context-free Class<T> mapper
            EventTransformer v1ToV2Transformer = EventTransformer.from(V1)
                                                                    .to(V2)
                                                                    .transform(JsonNode.class, jsonNode -> {
                                                                        ObjectNode v2 = JsonNodeFactory.instance.objectNode();
                                                                        v2.put("name", jsonNode.get("name").asText());
                                                                        return v2;
                                                                    });
            EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();

            // and a stored payload whose type differs from the declared input class
            EventMessage storedV1Event = new GenericEventMessage(V1, "{\"name\":\"Math 101\"}");
            EventStreamTestUtils.RecordingMessageConverter<JsonNode> recording =
                    recordingConverter(message -> parseJson((String) message.payload()));

            // when the event flows through the chain
            List<EventMessage> outputs = collectMessages(
                    chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, recording, RESOLVER));

            // then the context-free overload routes through the same converter logic as the BiFunction variant
            assertThat(recording.invocationCount()).isEqualTo(1);
            assertThat(recording.lastRequestedType()).isEqualTo(JsonNode.class);
            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
            assertThat(outputs.getFirst().payload())
                    .asInstanceOf(type(JsonNode.class))
                    .satisfies(node -> assertThat(node.get("name").asText()).isEqualTo("Math 101"));
        }

        @Test
        void typeReferenceOverloadPreservesParameterizedTypeWithoutAProcessingContext() {
            // given a transformation registered with a context-free TypeReference<T> mapper
            TypeReference<Map<String, Object>> mapType = new TypeReference<>() {
            };
            EventTransformer v1ToV2Transformer = EventTransformer.from(V1)
                                                                    .to(V2)
                                                                    .transform(mapType, payload -> {
                                                                        Map<String, Object> result = new HashMap<>(payload);
                                                                        result.put("upgraded", true);
                                                                        return result;
                                                                    });
            EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();

            EventMessage storedV1Event = new GenericEventMessage(V1, "{\"name\":\"Math 101\"}");
            EventStreamTestUtils.RecordingMessageConverter<Map<String, Object>> recording =
                    recordingConverter(message -> parseJsonToMap((String) message.payload()));

            // when the event flows through the chain
            List<EventMessage> outputs = collectMessages(
                    chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, recording, RESOLVER));

            // then the parameterized type still reaches the converter, identical to the BiFunction variant
            assertThat(recording.lastRequestedType())
                    .isInstanceOf(ParameterizedType.class)
                    .isEqualTo(mapType.getType());
            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
            assertThat(outputs.getFirst().payload())
                    .asInstanceOf(MAP)
                    .containsEntry("name", "Math 101")
                    .containsEntry("upgraded", true);
        }
    }

    @Nested
    final class MalformedPayload {

        @Test
        void converterReturningNullSurfacesAsIllegalStateExceptionIdentifyingTheEvent() {
            // When the converter resolves the stored payload to null (malformed / missing
            // persisted bytes), the chain MUST raise a clear error pinpointing the event.
            EventTransformer v1ToV2Transformer = EventTransformer.from(V1)
                                                                    .to(V2)
                                                                    .transform(JsonNode.class, (jsonNode, ctx) -> jsonNode);
            EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, "{}");
            EventStreamTestUtils.RecordingMessageConverter<JsonNode> nullReturningConverter =
                    recordingConverter(message -> null);
            MessageStream<EventMessage> transformed = chain.transform(
                    MessageStream.fromIterable(List.of(storedV1Event)), null, nullReturningConverter, RESOLVER);

            assertThatThrownBy(() -> collectMessages(transformed))
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .rootCause()
                    .hasMessageContaining("resolved the stored payload to null")
                    .hasMessageContaining(JsonNode.class.getTypeName())
                    .hasMessageContaining(storedV1Event.identifier())
                    .hasMessageContaining(V1.toString());
        }
    }

    private static JsonNode parseJson(String json) {
        try {
            return new ObjectMapper().readTree(json);
        } catch (Exception failure) {
            throw new AssertionError("test fixture failed to parse json", failure);
        }
    }

    private static Map<String, Object> parseJsonToMap(String json) {
        try {
            return new ObjectMapper().readValue(json, JACKSON_MAP_TYPE);
        } catch (Exception failure) {
            throw new AssertionError("test fixture failed to parse json to map", failure);
        }
    }

    private static final com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>> JACKSON_MAP_TYPE =
            new com.fasterxml.jackson.core.type.TypeReference<>() {
            };
}
