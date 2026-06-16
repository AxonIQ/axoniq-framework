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
import io.axoniq.framework.messaging.transformation.ChainConfigurationException;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Configuration is startup-only: once {@code build()} has been called, further configuration calls on the
 * builder ({@code register(...)}, {@code maxIterationsPerEvent(...)}) are rejected with a
 * {@link ChainConfigurationException}.
 */
final class EventTransformerChainLockingTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");

    @Test
    void registrationAfterBuildThrowsChainConfigurationException() {
        // given
        EventTransformerChain.Builder builder = EventTransformerChain.builder();
        builder.build();

        EventTransformation additionalTransformation =
                EventTransformation.from(V1).to(V2).transform(JsonNode.class, (in, ctx) -> in);

        // when / then
        assertThatThrownBy(() -> builder.register(additionalTransformation))
                .isInstanceOf(ChainConfigurationException.class)
                .hasMessageContaining("build");
    }

    @Test
    void maxIterationsPerEventAfterBuildThrowsChainConfigurationException() {
        // given
        EventTransformerChain.Builder builder = EventTransformerChain.builder();
        builder.build();

        // when / then
        assertThatThrownBy(() -> builder.maxIterationsPerEvent(50))
                .isInstanceOf(ChainConfigurationException.class)
                .hasMessageContaining("build");
    }
}
