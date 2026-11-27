/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.suite;

import io.axoniq.framework.workflow.runtime.execution.SegmentKeyRange;
import io.axoniq.framework.workflow.runtime.execution.WaitAssociation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitClosed;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitOpened;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtCleared;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtSet;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowEnded;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowStarted;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges.SegmentPosition;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexRows;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Tests a {@link WorkflowIndex} that keeps its rows in a database: which rows a segment and the workflow manager load,
 * what each {@link WorkflowIndexChange} and stored position do to the rows, and what an initialization adds. The JPA
 * and the JDBC index run it.
 *
 * @author Stefan Dragisic
 */
public abstract class WorkflowIndexTestSuite {

    private static final String MODULE = "orders";
    private static final Instant WAKES_AT = Instant.parse("2026-12-24T18:00:00Z");

    private WorkflowIndex testSubject;

    /**
     * Creates the index under test, on an empty database.
     *
     * @return the index under test
     */
    protected abstract WorkflowIndex createIndex();

    /**
     * Releases the database of the index under test.
     */
    protected abstract void closeIndex();

    @BeforeEach
    void setUp() {
        testSubject = createIndex();
        testSubject.initialize(MODULE, Set.of(), List.of()).orTimeout(5, TimeUnit.SECONDS).join();
    }

    @AfterEach
    void tearDown() {
        closeIndex();
    }

    @Nested
    class RowsOfASegment {

        @Test
        void areExactlyTheRowsOfTheWorkflowsTheSegmentOwns() {
            // given
            var workflowIds = IntStream.range(0, 200).mapToObj(i -> "order-" + i).toList();
            store(workflowIds.stream().flatMap(WorkflowIndexTestSuite::startedAndWaiting).toList());

            for (Segment segment : Segment.splitBalanced(Segment.ROOT_SEGMENT, 3)) {
                // when
                var rows = load(segment);

                // then
                var owned = workflowIds.stream().filter(segment::matches).toList();
                assertThat(rows.runningWorkflowIds()).containsExactlyInAnyOrderElementsOf(owned);
                assertThat(rows.waits()).extracting(WaitAssociation::workflowId)
                                        .containsExactlyInAnyOrderElementsOf(owned);
            }
        }

        @Test
        void areFoundAfterASplitAndAfterAMergeWithoutStoringThemAgain() {
            // given
            var workflowIds = IntStream.range(0, 200).mapToObj(i -> "order-" + i).toList();
            store(workflowIds.stream().flatMap(WorkflowIndexTestSuite::startedAndWaiting).toList());
            var segment = Segment.splitBalanced(Segment.ROOT_SEGMENT, 1).getFirst();
            var before = load(segment).runningWorkflowIds();

            // when
            var halves = segment.split();
            var merged = halves[0].mergedWith(halves[1]);

            // then
            var ofHalves = new HashSet<>(load(halves[0]).runningWorkflowIds());
            assertThat(ofHalves).doesNotContainAnyElementsOf(load(halves[1]).runningWorkflowIds());
            ofHalves.addAll(load(halves[1]).runningWorkflowIds());
            assertThat(ofHalves).isEqualTo(before);
            assertThat(load(merged).runningWorkflowIds()).isEqualTo(before);
        }

        @Test
        void holdNoRowOfAnotherModule() {
            // given
            store(startedAndWaiting("order-1").toList());
            testSubject.initialize("payments", Set.of(), List.of()).orTimeout(5, TimeUnit.SECONDS).join();

            // when
            var rows = testSubject.load("payments", Segment.ROOT_SEGMENT, null)
                                  .orTimeout(5, TimeUnit.SECONDS)
                                  .join()
                                  .orElseThrow();

            // then
            assertThat(rows.runningWorkflowIds()).isEmpty();
            assertThat(rows.waits()).isEmpty();
        }
    }

    @Nested
    class StoredChange {

        @Test
        void keepsTheEventNameAndTheAssociationsOfAnOpenWait() {
            // given
            var wait = new WaitAssociation("order-1",
                                           "awaitPaid",
                                           new QualifiedName("paid"),
                                           Set.of("payload:orderId=order-1", "payload:kind=card"));

            // when
            store(List.of(new WorkflowStarted("order-1"), new WaitOpened(wait)));

            // then
            assertThat(load(Segment.ROOT_SEGMENT).waits()).containsExactly(wait);
        }

        @Test
        void removesTheRowAndTheWaitsOfAnEndedWorkflow() {
            // given
            store(Stream.concat(startedAndWaiting("order-1"), startedAndWaiting("order-2")).toList());

            // when
            store(List.of(new WorkflowEnded("order-1")));

            // then
            var rows = load(Segment.ROOT_SEGMENT);
            assertThat(rows.runningWorkflowIds()).containsExactly("order-2");
            assertThat(rows.waits()).extracting(WaitAssociation::workflowId).containsExactly("order-2");
        }

        @Test
        void removesTheRowOfAClosedWait() {
            // given
            store(startedAndWaiting("order-1").toList());

            // when
            store(List.of(new WaitClosed("order-1", "awaitPaid")));

            // then
            var rows = load(Segment.ROOT_SEGMENT);
            assertThat(rows.runningWorkflowIds()).containsExactly("order-1");
            assertThat(rows.waits()).isEmpty();
        }

        @Test
        void hasTheSameOutcomeWhenItIsStoredTwice() {
            // given
            List<WorkflowIndexChange> changes = new ArrayList<>(startedAndWaiting("order-1").toList());
            changes.addAll(startedAndWaiting("order-2").toList());
            changes.add(new WakesAtSet("order-1", WAKES_AT));
            changes.add(new WaitClosed("order-2", "awaitPaid"));
            store(changes);
            var once = load(Segment.ROOT_SEGMENT);

            // when
            store(changes);

            // then
            assertThat(load(Segment.ROOT_SEGMENT)).isEqualTo(once);
            assertThat(once.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-2");
            assertThat(once.waits()).extracting(WaitAssociation::workflowId).containsExactly("order-1");
        }

        @Test
        void addsTheRowOfAWorkflowThatHibernatedBeforeItsStartWasStored() {
            // when
            store(List.of(new WakesAtSet("order-1", WAKES_AT)));

            // then
            var rows = load(Segment.ROOT_SEGMENT);
            assertThat(rows.runningWorkflowIds()).containsExactly("order-1");
            assertThat(rows.wakesAt()).containsEntry("order-1", WAKES_AT);
        }

        @Test
        void clearsTheWakeTimeOfAWorkflowThatWoke() {
            // given
            store(List.of(new WorkflowStarted("order-1"), new WakesAtSet("order-1", WAKES_AT)));

            // when
            store(List.of(new WakesAtCleared("order-1")));

            // then
            var rows = load(Segment.ROOT_SEGMENT);
            assertThat(rows.runningWorkflowIds()).containsExactly("order-1");
            assertThat(rows.wakesAt()).isEmpty();
        }

        @Test
        void addsNoRowForAWorkflowThatWokeAndEnded() {
            // given
            store(List.of(new WorkflowStarted("order-1"), new WorkflowEnded("order-1")));

            // when
            store(List.of(new WakesAtCleared("order-1")));

            // then
            assertThat(load(Segment.ROOT_SEGMENT).runningWorkflowIds()).isEmpty();
        }
    }

    @Nested
    class Initialization {

        @Test
        void isWhatLetsAModuleLoadItsRows() {
            // given
            testSubject.store("payments", new WorkflowIndexChanges(startedAndWaiting("order-1").toList()), null)
                       .orTimeout(5, TimeUnit.SECONDS)
                       .join();

            // when
            var beforeInitialization = loadPayments();
            testSubject.initialize("payments", Set.of(), List.of()).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(beforeInitialization).isEmpty();
            assertThat(loadPayments()).hasValueSatisfying(
                    rows -> assertThat(rows.runningWorkflowIds()).containsExactly("order-1")
            );
        }

        @Test
        void addsTheGivenWorkflowsAndTheirWaitsWhileKeepingTheStoredRows() {
            // given: a stored row with a wake time, and a stored wait
            testSubject.store("payments", new WorkflowIndexChanges(List.of(
                    new WakesAtSet("order-1", WAKES_AT),
                    new WaitOpened(wait("order-1", "payload:orderId=order-1"))
            )), null).orTimeout(5, TimeUnit.SECONDS).join();

            // when: an initialization with another wait of the same step, and a wait of a workflow that does not run
            testSubject.initialize("payments",
                                   Set.of("order-1", "order-2"),
                                   List.of(wait("order-1", "other"), wait("order-2", "payload:orderId=order-2"),
                                           wait("order-3", "payload:orderId=order-3")))
                       .orTimeout(5, TimeUnit.SECONDS)
                       .join();

            // then
            var rows = loadPayments().orElseThrow();
            assertThat(rows.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-2");
            assertThat(rows.wakesAt()).containsExactly(Map.entry("order-1", WAKES_AT));
            assertThat(rows.waits()).extracting(WaitAssociation::workflowId, WaitAssociation::associations)
                                    .containsExactlyInAnyOrder(
                                            tuple("order-1", Set.of("payload:orderId=order-1")),
                                            tuple("order-2", Set.of("payload:orderId=order-2"))
                                    );
        }

        @Test
        void canRunTwice() {
            // when
            testSubject.initialize("payments", Set.of("order-1"), List.of()).orTimeout(5, TimeUnit.SECONDS).join();
            testSubject.initialize("payments", Set.of("order-1"), List.of()).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(loadPayments()).hasValueSatisfying(
                    rows -> assertThat(rows.runningWorkflowIds()).containsExactly("order-1")
            );
        }

        private static WaitAssociation wait(String workflowId, String association) {
            return new WaitAssociation(workflowId, "awaitPaid", new QualifiedName("paid"), Set.of(association));
        }

        private Optional<WorkflowIndexRows> loadPayments() {
            return testSubject.load("payments", Segment.ROOT_SEGMENT, null).orTimeout(5, TimeUnit.SECONDS).join();
        }
    }

    @Nested
    class RowsOfWorkflows {

        @Test
        void areTheRowsOfTheGivenRunningWorkflowsInTheModuleWithEveryPositionOfTheModule() {
            // given
            var halves = Segment.ROOT_SEGMENT.split();
            testSubject.store(MODULE,
                              new WorkflowIndexChanges(List.of(new WorkflowStarted("order-1"),
                                                               new WakesAtSet("order-1", WAKES_AT),
                                                               new WorkflowStarted("order-2"),
                                                               new WorkflowEnded("order-2")),
                                                       position(halves[0], 42L)),
                              null)
                       .orTimeout(5, TimeUnit.SECONDS)
                       .join();
            storePosition(halves[1], 43L);
            testSubject.initialize("payments", Set.of("order-3"), List.of()).orTimeout(5, TimeUnit.SECONDS).join();

            // when
            var rows = loadWorkflows(List.of("order-1", "order-2", "order-3"));

            // then
            assertThat(rows.runningWorkflowIds()).containsExactly("order-1");
            assertThat(rows.wakesAt()).containsEntry("order-1", WAKES_AT);
            assertThat(rows.waits()).isEmpty();
            assertThat(rows.positions())
                    .containsExactlyInAnyOrder(position(halves[0], 42L), position(halves[1], 43L));
        }

        @Test
        void areFoundForMoreWorkflowsThanOneQueryTakes() {
            // given
            var workflowIds = IntStream.rangeClosed(1, 1_001).mapToObj(i -> "order-" + i).toList();
            store(workflowIds.stream().map(WorkflowStarted::new).toList());

            // when
            var rows = loadWorkflows(workflowIds);

            // then
            assertThat(rows.runningWorkflowIds()).containsExactlyInAnyOrderElementsOf(workflowIds);
        }

        @Test
        void areNotReportedForAModuleThatIsNotInitialized() {
            // when
            var rows = testSubject.loadWorkflows("payments", List.of("order-1"), null)
                                  .orTimeout(5, TimeUnit.SECONDS)
                                  .join();

            // then
            assertThat(rows).isEmpty();
        }
    }

    @Nested
    class StoredPosition {

        @Test
        void isKeptApartPerModule() {
            // given
            testSubject.initialize("payments", Set.of(), List.of()).orTimeout(5, TimeUnit.SECONDS).join();

            // when
            storePosition(Segment.ROOT_SEGMENT, 42L);

            // then
            assertThat(loadWorkflows(List.of()).positions()).containsExactly(position(Segment.ROOT_SEGMENT, 42L));
            assertThat(testSubject.loadWorkflows("payments", List.of(), null)
                                  .orTimeout(5, TimeUnit.SECONDS)
                                  .join()
                                  .orElseThrow()
                                  .positions()).isEmpty();
        }

        @Test
        void replacesTheStoredPositionOfTheSameSegmentEvenWithALowerOne() {
            // given
            storePosition(Segment.ROOT_SEGMENT, 46L);

            // when
            storePosition(Segment.ROOT_SEGMENT, 40L);

            // then
            assertThat(loadWorkflows(List.of()).positions()).containsExactly(position(Segment.ROOT_SEGMENT, 40L));
        }

        @Test
        void replacesThePositionsOfTheSegmentsAMergeReplaced() {
            // given
            var halves = Segment.ROOT_SEGMENT.split();
            storePosition(halves[0], 42L);
            storePosition(halves[1], 43L);

            // when
            storePosition(Segment.ROOT_SEGMENT, 44L);

            // then
            assertThat(loadWorkflows(List.of()).positions()).containsExactly(position(Segment.ROOT_SEGMENT, 44L));
        }

        @Test
        void replacesThePositionOfTheSegmentASplitReplaced() {
            // given
            var halves = Segment.ROOT_SEGMENT.split();
            storePosition(Segment.ROOT_SEGMENT, 44L);

            // when
            storePosition(halves[0], 45L);

            // then: the other half has no position until it stores one
            var rows = loadWorkflows(List.of());
            assertThat(rows.positions()).containsExactly(position(halves[0], 45L));
            assertThat(rows.positionOf(Segment.ROOT_SEGMENT)).isEmpty();
        }
    }

    private static Stream<WorkflowIndexChange> startedAndWaiting(String workflowId) {
        return Stream.of(new WorkflowStarted(workflowId),
                         new WaitOpened(new WaitAssociation(workflowId,
                                                            "awaitPaid",
                                                            new QualifiedName("paid"),
                                                            Set.of("payload:orderId=" + workflowId))));
    }

    private void store(List<? extends WorkflowIndexChange> changes) {
        testSubject.store(MODULE, new WorkflowIndexChanges(new ArrayList<>(changes)), null)
                   .orTimeout(5, TimeUnit.SECONDS)
                   .join();
    }

    private WorkflowIndexRows load(Segment segment) {
        return testSubject.load(MODULE, segment, null).orTimeout(5, TimeUnit.SECONDS).join().orElseThrow();
    }

    private WorkflowIndexRows loadWorkflows(List<String> workflowIds) {
        return testSubject.loadWorkflows(MODULE, workflowIds, null).orTimeout(5, TimeUnit.SECONDS).join().orElseThrow();
    }

    private void storePosition(Segment segment, long position) {
        testSubject.store(MODULE, new WorkflowIndexChanges(List.of(), position(segment, position)), null)
                   .orTimeout(5, TimeUnit.SECONDS)
                   .join();
    }

    private static SegmentPosition position(Segment segment, long position) {
        return new SegmentPosition(SegmentKeyRange.of(segment), position);
    }
}
