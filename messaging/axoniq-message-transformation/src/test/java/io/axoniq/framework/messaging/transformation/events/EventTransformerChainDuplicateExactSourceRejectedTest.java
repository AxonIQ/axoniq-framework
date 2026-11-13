/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.events;

import com.fasterxml.jackson.databind.JsonNode;
import io.axoniq.framework.messaging.transformation.ChainConfigurationException;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Two transformations may not claim the same exact source identity; the chain rejects such a configuration when
 * built, whether the duplicate comes from two single-version registrations or from a rename or drop that claims a
 * source another transformation already holds.
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
    void aDropAndARenameOnTheSameSourceAreRejected() {
        // given a drop and a rename that both claim V1 exactly
        EventTransformerChain.Builder builder = EventTransformerChain.builder()
                .register(EventTransformation.drop(V1))
                .register(EventTransformation.rename(V1, V2));

        // when the chain is built / then it is rejected, naming the conflicting source
        assertThatThrownBy(builder::build)
                .isInstanceOf(ChainConfigurationException.class)
                .hasMessageContaining(V1.toString());
    }

    @Test
    void twoRenamesOnTheSameSourceAreRejected() {
        // given two renames that both match V1 exactly
        EventTransformerChain.Builder builder = EventTransformerChain.builder()
                .register(EventTransformation.rename(V1, V2))
                .register(EventTransformation.rename(V1, V3));

        // when the chain is built / then it is rejected, naming the conflicting source
        assertThatThrownBy(builder::build)
                .isInstanceOf(ChainConfigurationException.class)
                .hasMessageContaining(V1.toString());
    }

    @Test
    void twoDropsOnTheSameSourceAreRejected() {
        // given two drops that both claim V1 exactly
        EventTransformerChain.Builder builder = EventTransformerChain.builder()
                .register(EventTransformation.drop(V1))
                .register(EventTransformation.drop(V1));

        // when the chain is built / then it is rejected, naming the conflicting source
        assertThatThrownBy(builder::build)
                .isInstanceOf(ChainConfigurationException.class)
                .hasMessageContaining(V1.toString());
    }
}
