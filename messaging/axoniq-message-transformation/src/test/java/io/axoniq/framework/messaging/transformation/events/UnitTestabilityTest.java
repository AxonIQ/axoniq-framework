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
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A transformation produced by the factory is unit-testable from a plain JUnit test by
 * registering it with a single-transformer {@link EventTransformerChain} and invoking
 * {@code chain.transform(...)}, the same entry point the framework's
 * {@code TransformingEventStore} decorator uses at production read time. No event store,
 * processor, or framework bootstrap is required.
 */
final class UnitTestabilityTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    @Test
    void transformationIsInvocableThroughASingleTransformerChain() {
        EventTransformer v1ToV2Transformer = EventTransformation.from(V1)
                                                                .to(V2)
                                                                .transform(JsonNode.class, (in, ctx) -> in.deepCopy());
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();
        EventMessage storedV1Event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        List<EventMessage> outputs = collectMessages(
                chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER, RESOLVER));

        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst().type()).isEqualTo(V2);
    }

    @Test
    @Disabled("Tests-first; impl lands with the rename factory entry point.")
    void renameTransformationIsInvocableThroughASingleTransformerChain() {
        EventTransformer renameTransformer = EventTransformation.rename(V1, V2);
        EventTransformerChain chain = EventTransformerChain.builder().register(renameTransformer).build();
        EventMessage storedV1Event = new GenericEventMessage(V1, "payload");

        List<EventMessage> outputs = collectMessages(
                chain.transform(MessageStream.fromIterable(List.of(storedV1Event)), null, CONVERTER, RESOLVER));

        assertThat(outputs).hasSize(1);
        assertThat(outputs.getFirst().type()).isEqualTo(V2);
        assertThat(outputs.getFirst().payload()).isEqualTo("payload");
    }
}
