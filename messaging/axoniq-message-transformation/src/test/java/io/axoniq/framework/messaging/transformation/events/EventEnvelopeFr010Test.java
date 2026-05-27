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
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the event envelope (message identifier, metadata, identity-related fields)
 * is preserved across transformation. A 1:1 transformer may rewrite payload and
 * {@link MessageType}, but framework-controlled envelope fields flow through unchanged.
 */
class EventEnvelopeFr010Test {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");

    @Test
    @Disabled("Tests-first; impl lands in T027 (chain matching path) + T030 (envelope preservation in TransformingEventStore)")
    void output_message_carries_same_identifier_as_input_after_a_one_to_one_transformation() {
        // given
        EventTransformer t = EventTransformation.from(V1)
                                                .to(V2)
                                                .transform(JsonNode.class, (in, ctx) -> JsonNodeFactory.instance.objectNode());
        EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

        EventMessage input = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        // when
        List<EventMessage> out = drain(chain.transform(MessageStream.fromIterable(List.of(input))));

        // then
        assertThat(out).hasSize(1);
        assertThat(out.get(0).identifier()).isEqualTo(input.identifier());
        assertThat(out.get(0).type()).isEqualTo(V2);
    }

    @Test
    @Disabled("Tests-first; impl lands in T027")
    void metadata_flows_forward_unchanged() {
        // given
        EventTransformer t = EventTransformation.from(V1)
                                                .to(V2)
                                                .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

        Metadata metadata = Metadata.from(Map.of("correlationId", "abc-123", "userId", "u-42"));
        EventMessage input = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode())
                .andMetadata(metadata);

        // when
        List<EventMessage> out = drain(chain.transform(MessageStream.fromIterable(List.of(input))));

        // then
        assertThat(out).hasSize(1);
        assertThat(out.get(0).metadata()).containsEntry("correlationId", "abc-123");
        assertThat(out.get(0).metadata()).containsEntry("userId", "u-42");
    }

    private static List<EventMessage> drain(MessageStream<? extends EventMessage> stream) {
        return stream.<List<EventMessage>>reduce(new ArrayList<>(), (acc, entry) -> {
            acc.add(entry.message());
            return acc;
        }).join();
    }
}
