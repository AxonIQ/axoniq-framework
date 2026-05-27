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
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a transformation produced by the factory is unit-testable from a plain
 * JUnit test with no event store, no processor, no framework bootstrap, and no
 * {@code ProcessingContext} -- the only dependency a user needs is the transformer itself.
 */
class UnitTestabilityTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");

    @Test
    @Disabled("Tests-first; impl lands in T023 + T024 (factory produces a directly-invocable EventTransformer)")
    void transformation_is_invocable_without_a_chain_or_event_store() {
        // given -- the bare transformer produced by the factory
        EventTransformer t = EventTransformation.from(V1)
                                                .to(V2)
                                                .transform(JsonNode.class, (in, ctx) -> in.deepCopy());

        EventMessage input = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        // when -- invoked directly, passing a null ProcessingContext
        MessageStream<? extends EventMessage> result = t.transform(input, null);

        // then -- single output, type updated to v2
        List<EventMessage> out = drain(result);
        assertThat(out).hasSize(1);
        assertThat(out.get(0).type()).isEqualTo(V2);
    }

    @Test
    @Disabled("Tests-first; impl lands in T038 (rename factory entry point)")
    void rename_transformation_is_invocable_without_a_payload_mapper() {
        // given
        EventTransformer t = EventTransformation.rename(V1, V2);

        EventMessage input = new GenericEventMessage(V1, "payload");

        // when
        List<EventMessage> out = drain(t.transform(input, null));

        // then
        assertThat(out).hasSize(1);
        assertThat(out.get(0).type()).isEqualTo(V2);
        assertThat(out.get(0).payload()).isEqualTo("payload");
    }

    private static List<EventMessage> drain(MessageStream<? extends EventMessage> stream) {
        return stream.<List<EventMessage>>reduce(new ArrayList<>(), (acc, entry) -> {
            acc.add(entry.message());
            return acc;
        }).join();
    }
}
