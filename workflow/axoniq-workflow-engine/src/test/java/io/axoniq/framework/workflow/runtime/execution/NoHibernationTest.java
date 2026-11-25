/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflowsClaimLoader.ClaimedIndex;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link NoHibernation} keeps the behavior of an engine without hibernation: no execution may hibernate, a
 * claim restores every running workflow, and a restore takes no lock of its workflow.
 *
 * @author Allard Buijze
 */
class NoHibernationTest {

    private static final String WORKFLOW_ID = "workflow-1";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final Hibernation hibernation = NoHibernation.INSTANCE;

    private static SegmentClaim claimWithAnExecutionContext() {
        var segmentClaims = new SegmentClaims();
        segmentClaims.initialize(null);
        return segmentClaims.startClaim(Segment.ROOT_SEGMENT, new StubProcessingContext());
    }

    private static HibernationHint hint() {
        return new HibernationHint(Instant.now().plus(Duration.ofHours(1)), List.of());
    }

    @Test
    void isNotEnabledAndReportsNoHibernatedWorkflow() {
        assertThat(hibernation.isEnabled()).isFalse();
        assertThat(hibernation.hibernatedWorkflowIds()).isEmpty();
    }

    @Test
    void letsNoExecutionHibernateAlsoUnderAClaimWithAnExecutionContext() {
        // given
        var claim = claimWithAnExecutionContext();
        assertThat(claim.executionContext()).isNotNull();

        // when / then
        assertThat(hibernation.mayHibernate(claim)).isFalse();
        assertThat(hibernation.mayHibernate(null)).isFalse();
    }

    @Nested
    class Claims {

        @Test
        void restoreEveryRunningWorkflowAndClearTheHintsOfTheHibernatedOnes() {
            // given - a claim that loaded one running workflow, and one with a hibernation hint
            var claimed = new ClaimedIndex(Set.of("running"), Map.of(WORKFLOW_ID, hint()), Map.of());

            // when
            var accepted = hibernation.withAcceptedHints(claimed);

            // then
            assertThat(accepted.hibernated()).isEmpty();
            assertThat(accepted.runningWorkflowIds()).containsExactlyInAnyOrder("running", WORKFLOW_ID);
            assertThat(accepted.changes()).containsEntry(WORKFLOW_ID, true);
        }

        @Test
        void adoptNothing() {
            // when
            hibernation.adopt(claimWithAnExecutionContext(), Map.of(WORKFLOW_ID, hint()));

            // then
            assertThat(hibernation.hibernatedWorkflowIds()).isEmpty();
        }
    }

    @Nested
    class Restores {

        private final LiveWorkflows liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());

        @Test
        void runWithoutTheLockOfTheWorkflow() throws Exception {
            // given - another thread holds the lock of the workflow, as a hibernation in progress would
            var locked = new CountDownLatch(1);
            var unlock = new CountDownLatch(1);
            Thread.ofPlatform().start(() -> {
                try (var ignored = liveWorkflows.lock(WORKFLOW_ID)) {
                    locked.countDown();
                    unlock.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(locked.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();

            try {
                // when
                var restored = CompletableFuture.supplyAsync(
                        () -> hibernation.restoreGuarded(WORKFLOW_ID, () -> "restored")
                );

                // then - it does not wait for the lock
                assertThat(restored).succeedsWithin(TIMEOUT).isEqualTo("restored");
            } finally {
                unlock.countDown();
            }
        }

        @Test
        void neverTakeThePlaceOfAHibernation() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);

            // when
            boolean restoredOver = hibernation.restoreOverHibernation(
                    claimWithAnExecutionContext(), WORKFLOW_ID, execution, false
            );

            // then - the caller brings the execution into memory itself
            assertThat(restoredOver).isFalse();
        }
    }
}
