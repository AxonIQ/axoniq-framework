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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Two transformations may not claim the same exact source identity; the chain rejects such a configuration when
 * built, whether the duplicate comes from two single-version registrations or from a list that overlaps another.
 */
final class EventTransformerChainDuplicateExactSourceRejectedTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");
    private static final MessageType V3 = new MessageType("com.example.CourseCreated", "3.0.0");

    @Test
    void twoExactTransformationsOnTheSameSourceAreRejected() {
        // given two transformations that both match V1 exactly
        EventTransformerChain.Builder builder = EventTransformerChain.builder()
                .register(EventTransformation.from(V1).to(V2).transform(JsonNode.class, (in, ctx) -> in))
                .register(EventTransformation.from(V1).to(V3).transform(JsonNode.class, (in, ctx) -> in));

        // when the chain is built / then it is rejected, naming the conflicting source
        assertThatThrownBy(builder::build)
                .isInstanceOf(ChainConfigurationException.class)
                .hasMessageContaining(V1.toString());
    }

    @Test
    void aListOverlappingAnotherExactSourceIsRejected() {
        // given a list covering V1 and V2 alongside a single exact transformation on V2
        EventTransformerChain.Builder builder = EventTransformerChain.builder()
                .register(EventTransformation.from(List.of(V1, V2)).to(V3).transform(JsonNode.class, (in, ctx) -> in))
                .register(EventTransformation.from(V2).to(V3).transform(JsonNode.class, (in, ctx) -> in));

        // when the chain is built / then it is rejected on the overlapping V2
        assertThatThrownBy(builder::build)
                .isInstanceOf(ChainConfigurationException.class)
                .hasMessageContaining(V2.toString());
    }

    @Test
    void twoListsOverlappingOnAVersionAreRejected() {
        // given two multi-version registrations that both include V2
        EventTransformerChain.Builder builder = EventTransformerChain.builder()
                .register(EventTransformation.from(List.of(V1, V2)).to(V3).transform(JsonNode.class, (in, ctx) -> in))
                .register(EventTransformation.from(List.of(V2, V3)).to(V1).transform(JsonNode.class, (in, ctx) -> in));

        // when the chain is built / then it is rejected on the shared V2
        assertThatThrownBy(builder::build)
                .isInstanceOf(ChainConfigurationException.class)
                .hasMessageContaining(V2.toString());
    }
}
