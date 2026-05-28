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
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The chain enforces a per-event iteration bound. Default is
 * {@link EventTransformerChain#DEFAULT_MAX_ITERATIONS_PER_EVENT}; deep-history domains can
 * raise it via {@link EventTransformerChain.Builder#maxIterationsPerEvent(int)}.
 */
final class ChainSafetyBoundTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    @Test
    void exceedingTheConfiguredBoundRaisesChainConfigurationExceptionNamingTheOverride() {
        EventTransformer v1ToV2 = EventTransformation.from(V1).to(V2)
                                                     .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformer v2ToV1 = EventTransformation.from(V2).to(V1)
                                                     .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformerChain cyclingChain = EventTransformerChain.builder()
                                                                  .maxIterationsPerEvent(3)
                                                                  .register(v1ToV2)
                                                                  .register(v2ToV1)
                                                                  .build();
        EventMessage event = new GenericEventMessage(V1, JsonNodeFactory.instance.objectNode());

        assertThatThrownBy(() -> collectMessages(cyclingChain.transform(MessageStream.fromIterable(List.of(event)), null, CONVERTER, RESOLVER)))
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(ChainConfigurationException.class)
                .hasMessageContaining("exceeded 3 iterations")
                .hasMessageContaining("maxIterationsPerEvent");
    }

    @Test
    void builderRejectsNonPositiveMaxIterations() {
        assertThatThrownBy(() -> EventTransformerChain.builder().maxIterationsPerEvent(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxIterationsPerEvent must be >= 1");

        assertThatThrownBy(() -> EventTransformerChain.builder().maxIterationsPerEvent(-5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxIterationsPerEvent must be >= 1");
    }
}
