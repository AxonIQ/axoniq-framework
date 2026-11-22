/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.api;

import io.axoniq.framework.workflow.index.api.WorkflowIndex.IndexDelta;
import org.junit.jupiter.api.*;

import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link IndexDelta}.
 *
 * @author Allard Buijze
 */
class WorkflowIndexDeltaTest {

    @Test
    void rejectsAnIdThatIsBothAddedAndRemoved() {
        assertThatThrownBy(() -> new IndexDelta(Set.of("order-1", "order-2"), Set.of("order-2")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("order-2");
    }

    @Test
    void acceptsDisjointAddedAndRemovedIds() {
        // when
        var delta = new IndexDelta(Set.of("order-1"), Set.of("order-2"));

        // then
        assertThat(delta.added()).containsExactly("order-1");
        assertThat(delta.removed()).containsExactly("order-2");
    }
}
