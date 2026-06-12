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
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the chain threads the active {@link ProcessingContext} through to the user's
 * payload mapper. This is the single entry point the framework supplies the
 * {@code MessageConverter}, {@code MessageTypeResolver}, and processing context
 */
final class ChainContextPropagationTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");

    @Test
    void chainForwardsTheActiveProcessingContextToTheUsersMapper() {
        AtomicReference<@Nullable ProcessingContext> seenContext = new AtomicReference<>();
        EventTransformer v1ToV2Transformer = EventTransformer.from(V1).to(V2)
                .transform(JsonNode.class, (in, ctx) -> {
                    seenContext.set(ctx);
                    return in.deepCopy();
                });
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();
        EventMessage input = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());
        var realContext = StubProcessingContext.forMessage(input);

        collectMessages(chain.transform(
                MessageStream.fromIterable(List.of(input)),
                realContext,
                EventStreamTestUtils.neverInvokedConverter(),
                EventStreamTestUtils.alwaysEmptyMessageTypeResolver()));

        assertThat(seenContext.get()).isSameAs(realContext);
    }
}
