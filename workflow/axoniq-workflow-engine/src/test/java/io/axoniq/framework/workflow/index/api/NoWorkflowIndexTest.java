/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.api;

import io.axoniq.framework.workflow.index.api.WorkflowIndex.IndexDelta;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link WorkflowIndex#none()}.
 *
 * @author Allard Buijze
 */
class NoWorkflowIndexTest {

    private final WorkflowIndex workflowIndex = WorkflowIndex.none();

    @Test
    void neverReportsItselfInitialized() {
        // given
        workflowIndex.initialize("orders", Set.of("order-1")).orTimeout(5, TimeUnit.SECONDS).join();
        workflowIndex.store("orders", new IndexDelta(Set.of("order-2"), Set.of())).orTimeout(5, TimeUnit.SECONDS)
                     .join();

        // when
        var index = workflowIndex.load("orders", Segment.ROOT_SEGMENT).orTimeout(5, TimeUnit.SECONDS).join();

        // then - every claim reads the running workflows from the event store
        assertThat(index).isEmpty();
    }
}
