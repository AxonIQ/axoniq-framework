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
package io.axoniq.workflow.runtime.execution;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link WorkflowEngineCheckpointingSupport}.
 */
class WorkflowEngineCheckpointingSupportTest {

    @Test
    void waitsForTheInlineBarrierBeforeCompletingCheckpointAdvancement() {
        var coordinator = new InlineCheckpointBarrierCoordinator();
        coordinator.pendingWork = true;
        var support = new WorkflowEngineCheckpointingSupport(coordinator);
        var requested = new GlobalSequenceTrackingToken(42);

        var result = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

        assertThat(result).isNotDone();
        assertThat(coordinator.barrier).isNotNull();

        coordinator.pendingWork = false;
        coordinator.crossBarrier();

        assertThat(result).isCompletedWithValue(requested);
    }

    @Test
    void completesImmediatelyWhenNoCheckpointWorkIsPending() {
        var support = new WorkflowEngineCheckpointingSupport(new InlineCheckpointBarrierCoordinator());
        var requested = new GlobalSequenceTrackingToken(42);

        var result = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

        assertThat(result).isCompletedWithValue(requested);
    }

    private static final class InlineCheckpointBarrierCoordinator
            implements WorkflowEngineCheckpointingSupport.CheckpointBarrierCoordinator {

        private boolean pendingWork;
        private Runnable barrier;

        @Override
        public boolean hasPendingCheckpointWork() {
            return pendingWork;
        }

        @Override
        public boolean scheduleCheckpointIntent(@Nonnull Runnable onDrained) {
            barrier = onDrained;
            return true;
        }

        private void crossBarrier() {
            barrier.run();
        }
    }
}
