/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WaitAssociation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexRows;
import io.axoniq.framework.workflow.runtime.execution.WorkflowStore;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

/**
 * Runs the tests of {@link WorkflowConfigurerHibernationTest} with a {@link WorkflowIndex}, so a hibernated workflow is
 * kept out of memory across a segment claim by the row of the index instead of by a recorded snapshot. Adds the tests
 * of what the engine keeps in the rows, and of how a claim uses them.
 *
 * @author Stefan Dragisic
 */
class WorkflowConfigurerHibernationWithWorkflowIndexTest extends WorkflowConfigurerHibernationTest {

    // Shared by the configurations of one test, so a restarted node reads the rows of the stopped one.
    private WorkflowIndex index = new InMemoryWorkflowIndex();
    // Whether the next configuration gets the index, so a test can start a node without one.
    private boolean indexed = true;
    // The store of the configuration started last, as a spy that shows what a claim sourced.
    private @Nullable WorkflowStore workflowStore;

    @Override
    void configure(WorkflowConfigurer configurer) {
        if (indexed) {
            configurer.registerWorkflowIndex(configuration -> index);
        }
        configurer.componentRegistry(registry -> registry.registerDecorator(
                WorkflowStore.class, 0, (configuration, name, delegate) -> workflowStore = spy(delegate)
        ));
    }

    @Nested
    class RowsOfTheIndex {

        @Test
        void holdARunningWorkflowAndItsOpenWait() {
            // given
            var configuration = start(false);

            // when
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);

            // then
            awaitRowsOfWaitingWorkflow(workflowId);
        }

        @Test
        void loseAWorkflowThatCompleted() {
            // given
            var configuration = start(false);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitRowsOfWaitingWorkflow(workflowId);

            // when
            publish(configuration, "paid", "order-1", "");
            awaitWorkflowStatus(configuration, workflowId, WorkflowStatus.COMPLETED);

            // then
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                var rows = rows();
                assertThat(rows.runningWorkflowIds()).isEmpty();
                assertThat(rows.waits()).isEmpty();
            });
        }

        @Test
        void carryTheWakeTimeOfAHibernatedWorkflowWhileItsNodeStillRuns() {
            // given
            var configuration = start(true);
            var workflowId = startWorkflow(configuration, "wait", "order-1", WAIT_STEP);
            awaitHibernated(configuration, workflowId);

            // when: an event of another order moves the stored tokens
            publish(configuration, "paid", "order-2", "");

            // then: no segment was released, so a node that dies now leaves the wake time behind
            await().atMost(20, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(rows().wakesAt()).containsKey(workflowId));
        }

        @Test
        void loseTheWakeTimeOfAWorkflowThatAClaimLoaded() {
            // given: a hibernated workflow whose row carries its wake time, on a node that stops
            var stopped = start(true);
            var workflowId = startWorkflow(stopped, "wait", "order-1", WAIT_STEP);
            awaitHibernated(stopped, workflowId);
            stopped.shutdown();
            assertThat(rows().wakesAt()).containsKey(workflowId);

            // when: a node without hibernation claims the segment, so it loads the workflow, and stops
            var restarted = start(false);
            await().atMost(10, TimeUnit.SECONDS).until(() -> inMemory(restarted, workflowId));
            restarted.shutdown();

            // then: the row no longer says hibernated, so no later claim keeps the running workflow out of memory
            assertThat(rows().runningWorkflowIds()).containsExactly(workflowId);
            assertThat(rows().wakesAt()).isEmpty();
        }

        @Test
        void loseTheWakeTimeOfAWorkflowThatASourcedClaimLoaded() {
            // given: a hibernated workflow whose row carries its wake time, on a node that stops
            var stopped = start(true);
            var workflowId = startWorkflow(stopped, "wait", "order-1", WAIT_STEP);
            awaitHibernated(stopped, workflowId);
            stopped.shutdown();
            assertThat(rows().wakesAt()).containsKey(workflowId);

            // when: the next node cannot read the rows, so its claim sources and loads the workflow, and it stops
            var readable = index;
            index = spy(readable);
            doReturn(CompletableFuture.failedFuture(new IllegalStateException("The rows cannot be read.")))
                    .when(index).load(any(), any(), any());
            var restarted = start(false);
            await().atMost(10, TimeUnit.SECONDS).until(() -> inMemory(restarted, workflowId));
            restarted.shutdown();
            index = readable;

            // then: the row no longer says hibernated, so no later claim keeps the workflow out of memory
            assertThat(rows().runningWorkflowIds()).containsExactly(workflowId);
            assertThat(rows().wakesAt()).isEmpty();
        }

        @Test
        void carryNoWakeTimeWhenTheIndexDoesNotStoreWithTheToken() {
            // given
            index = new InMemoryWorkflowIndex(false);
            var stopped = startRecordingHibernatedWorkflows();
            var workflowId = startWorkflow(stopped, "wait", "order-1", WAIT_STEP);
            awaitHibernated(stopped, workflowId);

            // when
            stopped.shutdown();

            // then: the release stored the rows, and the next node adopts the workflow from the record of the
            // stopped node, as it does without an index
            assertThat(rows().runningWorkflowIds()).containsExactly(workflowId);
            assertThat(rows().wakesAt()).isEmpty();
            var restarted = startRecordingHibernatedWorkflows();
            awaitAdopted(restarted, workflowId);
        }
    }

    @Nested
    class ClaimOfASegment {

        @Test
        void readsItsRowsInsteadOfSourcingTheRunningWorkflowsAndTheOpenWaits() {
            // given: a waiting workflow on a node that stops
            var stopped = start(false);
            var workflowId = startWorkflow(stopped, "wait", "order-1", WAIT_STEP);
            awaitRowsOfWaitingWorkflow(workflowId);
            stopped.shutdown();

            // when
            var restarted = start(false);

            // then: the workflow is restored, and neither list was sourced for it
            await().atMost(10, TimeUnit.SECONDS).until(() -> inMemory(restarted, workflowId));
            verify(workflowStore, never()).loadRunningWorkflows(any());
            verify(workflowStore, never()).loadWaitAssociations(any());
        }

        @Test
        void sourcesTheRunningWorkflowsAndTheOpenWaitsWhenItsRowsCannotBeRead() {
            // given: a waiting workflow on a node that stops, and rows that cannot be read from then on
            var stopped = start(false);
            var workflowId = startWorkflow(stopped, "wait", "order-1", WAIT_STEP);
            awaitRowsOfWaitingWorkflow(workflowId);
            stopped.shutdown();
            index = spy(index);
            doReturn(CompletableFuture.failedFuture(new IllegalStateException("The rows cannot be read.")))
                    .when(index).load(any(), any(), any());

            // when
            var restarted = start(false);

            // then: the claim sourced both lists instead, and restored the workflow
            await().atMost(10, TimeUnit.SECONDS).until(() -> inMemory(restarted, workflowId));
            verify(workflowStore, atLeastOnce()).loadRunningWorkflows(any());
            verify(workflowStore, atLeastOnce()).loadWaitAssociations(any());
        }

        @Test
        void completesTheIndexFromTheRunningWorkflowsAndTheOpenWaitsOnFirstUse() {
            // given: a waiting workflow, started on a node without an index
            indexed = false;
            var withoutIndex = start(false);
            var workflowId = startWorkflow(withoutIndex, "wait", "order-1", WAIT_STEP);
            withoutIndex.shutdown();
            assertThat(rows().runningWorkflowIds()).isEmpty();

            // when: the next node has an index, which holds no row yet
            indexed = true;
            var withIndex = start(false);

            // then: its claim sourced both lists, restored the workflow, and stored what it sourced as rows
            await().atMost(10, TimeUnit.SECONDS).until(() -> inMemory(withIndex, workflowId));
            awaitRowsOfWaitingWorkflow(workflowId);
            assertThat(index.isComplete(MODULE, null).orTimeout(5, TimeUnit.SECONDS).join()).isTrue();
        }
    }

    private void awaitRowsOfWaitingWorkflow(String workflowId) {
        await().atMost(10, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            var rows = rows();
            assertThat(rows.runningWorkflowIds()).containsExactly(workflowId);
            assertThat(rows.waits()).extracting(WaitAssociation::workflowId, WaitAssociation::stepName)
                                    .containsExactly(tuple(workflowId, WAIT_STEP));
        });
    }

    /**
     * Returns the rows of every segment, read the way a claim of each segment reads its own.
     */
    private WorkflowIndexRows rows() {
        var running = new HashSet<String>();
        var wakesAt = new HashMap<String, Instant>();
        var waits = new ArrayList<WaitAssociation>();
        for (Segment segment : Segment.splitBalanced(Segment.ROOT_SEGMENT, SEGMENT_COUNT - 1)) {
            var rows = index.load(MODULE, segment, null).orTimeout(5, TimeUnit.SECONDS).join();
            running.addAll(rows.runningWorkflowIds());
            wakesAt.putAll(rows.wakesAt());
            waits.addAll(rows.waits());
        }
        return new WorkflowIndexRows(running, wakesAt, waits);
    }
}
