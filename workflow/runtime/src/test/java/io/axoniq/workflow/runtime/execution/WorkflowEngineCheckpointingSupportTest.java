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
        var coordinator = new InlineCheckpointLatchCoordinator();
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
        var support = new WorkflowEngineCheckpointingSupport(new InlineCheckpointLatchCoordinator());
        var requested = new GlobalSequenceTrackingToken(42);

        var result = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

        assertThat(result).isCompletedWithValue(requested);
    }

    @Test
    void retriesCheckpointBarrierWhenWorkAppearsAfterAnEmptyBarrierSchedule() {
        var coordinator = new ConcurrentWorkCoordinator();
        var support = new WorkflowEngineCheckpointingSupport(coordinator);
        var requested = new GlobalSequenceTrackingToken(42);

        var result = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

        assertThat(result).isCompletedWithValue(requested);
        assertThat(coordinator.scheduledBarriers).isEqualTo(2);
    }

    private static final class InlineCheckpointLatchCoordinator
            implements WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator {

        private boolean pendingWork;
        private Runnable barrier;
        private int scheduledBarriers;

        @Override
        public boolean hasUnsafeCheckpointWork(@Nonnull Segment segment) {
            return pendingWork;
        }

        @Override
        public void addCheckpointLatch(@Nonnull Segment segment, @Nonnull Runnable latch) {
            barrier = latch;
            scheduledBarriers++;
        }

        private void crossBarrier() {
            barrier.run();
        }
    }

    private static final class ConcurrentWorkCoordinator
            implements WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator {

        private boolean workAppended;
        private int scheduledBarriers;

        @Override
        public boolean hasUnsafeCheckpointWork(@Nonnull Segment segment) {
            return scheduledBarriers == 0 || workAppended;
        }

        @Override
        public void addCheckpointLatch(@Nonnull Segment segment, @Nonnull Runnable latch) {
            scheduledBarriers++;
            // The first schedule sees an empty snapshot. A workflow thread appends work before the callback re-checks.
            workAppended = scheduledBarriers == 1;
            latch.run();
        }
    }
}
