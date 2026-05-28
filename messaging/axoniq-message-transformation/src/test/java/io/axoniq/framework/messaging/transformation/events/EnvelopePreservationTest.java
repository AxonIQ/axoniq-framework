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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The event envelope (message identifier, metadata, identity-related fields) is
 * preserved across transformation. A 1:1 transformer may rewrite payload and
 * {@link MessageType}, but framework-controlled envelope fields flow through unchanged.
 */
final class EnvelopePreservationTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");

    @Test
    void outputMessageCarriesSameIdentifierAsInputAfterOneToOneTransformation() {
        EventTransformer v1ToV2Transformer = EventTransformation.from(V1)
                                                                .to(V2)
                                                                .transform(JsonNode.class, (in, ctx) -> JsonNodeFactory.instance.objectNode());
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();
        EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(List.of(storedV1Event))));

        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst().identifier()).isEqualTo(storedV1Event.identifier());
        assertThat(outputs.getFirst().type()).isEqualTo(V2);
    }

    @Test
    void metadataFlowsForwardUnchanged() {
        EventTransformer v1ToV2Transformer = EventTransformation.from(V1)
                                                                .to(V2)
                                                                .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();
        Metadata metadata = Metadata.from(Map.of("correlationId", "abc-123", "userId", "u-42"));
        EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode())
                .andMetadata(metadata);

        List<EventMessage> outputs = collectMessages(chain.transform(MessageStream.fromIterable(List.of(storedV1Event))));

        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst().metadata()).containsEntry("correlationId", "abc-123");
        assertThat(outputs.getFirst().metadata()).containsEntry("userId", "u-42");
    }
}
