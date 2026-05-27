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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the resolver-permitting output-identity check (T028). For 1:1 transformations
 * with a payload mapper, the framework attempts to resolve the output payload's
 * {@link MessageType} via {@code MessageTypeResolver.resolve(Class<?>)} and compares it
 * to the declared {@code to}. Three cases:
 * (a) typed POJO mismatch -> raises; (b) typed POJO match -> no exception;
 * (c) untyped output (JsonNode / Map / raw bytes) -> resolver returns empty, check is skipped.
 */
class EventTransformerChainFr018Test {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");

    @Nested
    class PojoOutput {

        @Test
        @Disabled("Tests-first; impl lands in T028 (FR-018 identity check via MessageTypeResolver)")
        void mismatch_between_declared_to_and_resolved_pojo_message_type_raises() {
            // given -- a transformation whose mapper returns a POJO whose @Event-resolved
            // MessageType differs from the declared `to`
            EventTransformer t = EventTransformation.from(V1)
                                                    .to(V2)
                                                    .transform(JsonNode.class, (in, ctx) -> new WrongTypePojo());
            EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

            EventMessage input = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

            // when / then -- the mismatch surfaces with declared-to, actual-output, and stream position
            assertThatThrownBy(() -> drain(chain.transform(MessageStream.fromIterable(List.of(input)))))
                    .isInstanceOf(ChainConfigurationException.class)
                    .hasMessageContaining(V2.toString())
                    .hasMessageContaining(WrongTypePojo.class.getName());
        }

        @Test
        @Disabled("Tests-first; impl lands in T028")
        void match_between_declared_to_and_resolved_pojo_message_type_passes_silently() {
            // given -- a transformation whose mapper returns a POJO matching V2's identity
            EventTransformer t = EventTransformation.from(V1)
                                                    .to(V2)
                                                    .transform(JsonNode.class, (in, ctx) -> new SamplePojoV2());
            EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

            EventMessage input = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

            // when / then -- no exception
            List<EventMessage> out = drain(chain.transform(MessageStream.fromIterable(List.of(input))));
            assertThat(out).hasSize(1);
        }
    }

    @Nested
    class UntypedOutput {

        @Test
        @Disabled("Tests-first; impl lands in T028 (resolver returns Optional.empty for JsonNode / Map)")
        void jsonnode_output_skips_identity_check_silently() {
            // given -- mapper returns a JsonNode (no class-level identity annotation)
            EventTransformer t = EventTransformation.from(V1)
                                                    .to(V2)
                                                    .transform(JsonNode.class, (in, ctx) -> JsonNodeFactory.instance.objectNode());
            EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

            EventMessage input = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

            // when / then -- check is skipped (resolver returns empty); no exception
            List<EventMessage> out = drain(chain.transform(MessageStream.fromIterable(List.of(input))));
            assertThat(out).hasSize(1);
            assertThat(out.get(0).type()).isEqualTo(V2);
        }

        @Test
        @Disabled("Tests-first; impl lands in T028")
        void map_output_skips_identity_check_silently() {
            // given -- mapper returns a Map
            EventTransformer t = EventTransformation.from(V1)
                                                    .to(V2)
                                                    .transform(JsonNode.class, (in, ctx) -> {
                                                        Map<String, Object> result = new HashMap<>();
                                                        result.put("key", "value");
                                                        return result;
                                                    });
            EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

            EventMessage input = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

            // when / then -- check is skipped
            List<EventMessage> out = drain(chain.transform(MessageStream.fromIterable(List.of(input))));
            assertThat(out).hasSize(1);
            assertThat(out.get(0).type()).isEqualTo(V2);
        }
    }

    /** Helper POJO whose class differs from any registered MessageType. */
    private static final class WrongTypePojo {
    }

    /** Helper POJO that would resolve to V2 once @Event-annotated (impl detail of T028). */
    private static final class SamplePojoV2 {
    }

    private static List<EventMessage> drain(MessageStream<? extends EventMessage> stream) {
        return stream.<List<EventMessage>>reduce(new ArrayList<>(), (acc, entry) -> {
            acc.add(entry.message());
            return acc;
        }).join();
    }
}
