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
import io.axoniq.framework.messaging.transformation.ChainConfigurationException;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Resolver-permitting output-identity check for 1:1 transformations with a payload mapper.
 * The framework attempts to resolve the output payload's {@link MessageType} via
 * {@code MessageTypeResolver.resolve(Class<?>)} and compares it to the declared {@code to}.
 * Three cases: (a) typed POJO mismatch -> raises; (b) typed POJO match -> no exception;
 * (c) untyped output (JsonNode / Map / raw bytes) -> resolver returns empty, check is skipped.
 */
final class OutputIdentityCheckTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();

    @Nested
    final class PojoOutput {

        @Test
        @Disabled("Tests-first; impl lands in T028 (identity check via MessageTypeResolver)")
        void mismatchBetweenDeclaredToAndResolvedPojoMessageTypeRaises() {
            EventTransformer wrongTypeProducingTransformer = EventTransformation.from(V1)
                                                                                .to(V2)
                                                                                .transform(JsonNode.class, (in, ctx) -> new WrongTypePojo());
            EventTransformerChain chain = EventTransformerChain.builder().register(wrongTypeProducingTransformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

            assertThatThrownBy(() -> collectMessages(chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER)))
                    .isInstanceOf(ChainConfigurationException.class)
                    .hasMessageContaining(V2.toString())
                    .hasMessageContaining(WrongTypePojo.class.getName());
        }

        @Test
        @Disabled("Tests-first; impl lands in T028")
        void matchBetweenDeclaredToAndResolvedPojoMessageTypePassesSilently() {
            EventTransformer matchingPojoTransformer = EventTransformation.from(V1)
                                                                          .to(V2)
                                                                          .transform(JsonNode.class, (in, ctx) -> new SamplePojoV2());
            EventTransformerChain chain = EventTransformerChain.builder().register(matchingPojoTransformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

            List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER));

            assertThat(outputs).hasSize(1);
        }
    }

    @Nested
    final class UntypedOutput {

        @Test
        @Disabled("Tests-first; impl lands in T028 (resolver returns Optional.empty for JsonNode / Map)")
        void jsonNodeOutputSkipsIdentityCheckSilently() {
            EventTransformer jsonNodeProducingTransformer = EventTransformation.from(V1)
                                                                               .to(V2)
                                                                               .transform(JsonNode.class, (in, ctx) -> JsonNodeFactory.instance.objectNode());
            EventTransformerChain chain = EventTransformerChain.builder().register(jsonNodeProducingTransformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

            List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER));

            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
        }

        @Test
        @Disabled("Tests-first; impl lands in T028")
        void mapOutputSkipsIdentityCheckSilently() {
            EventTransformer mapProducingTransformer = EventTransformation.from(V1)
                                                                          .to(V2)
                                                                          .transform(JsonNode.class, (in, ctx) -> {
                                                                              Map<String, Object> result = new HashMap<>();
                                                                              result.put("key", "value");
                                                                              return result;
                                                                          });
            EventTransformerChain chain = EventTransformerChain.builder().register(mapProducingTransformer).build();
            EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

            List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER));

            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(V2);
        }
    }

    /** Helper POJO whose class differs from any registered MessageType. */
    private static final class WrongTypePojo {
    }

    /** Helper POJO that would resolve to V2 once {@code @Event}-annotated. */
    private static final class SamplePojoV2 {
    }
}
