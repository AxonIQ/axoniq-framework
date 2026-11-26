/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import io.axoniq.framework.workflow.runtime.execution.WaitAssociation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitClosed;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitOpened;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtCleared;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtSet;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowEnded;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowStarted;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexRows;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
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
 * Tests the {@link JpaWorkflowIndex} against an in-memory database: which rows a segment loads, what each
 * {@link WorkflowIndexChange} does to the rows, and what an initialization adds.
 *
 * @author Stefan Dragisic
 */
class JpaWorkflowIndexTest {

    private static final String MODULE = "orders";
    private static final Instant WAKES_AT = Instant.parse("2026-12-24T18:00:00Z");

    private EntityManagerFactory entityManagerFactory;
    private JpaWorkflowIndex testSubject;

    @BeforeEach
    void setUp() {
        // The schema is created with the factory and dropped with it, so every test starts with empty tables.
        entityManagerFactory = Persistence.createEntityManagerFactory("workflow-index");
        testSubject = new JpaWorkflowIndex(new JpaTransactionalExecutorProvider(entityManagerFactory),
                                           new JacksonConverter(),
                                           JpaWorkflowIndexConfiguration.DEFAULT);
        testSubject.initialize(MODULE, Set.of(), List.of()).orTimeout(5, TimeUnit.SECONDS).join();
    }

    @AfterEach
    void tearDown() {
        entityManagerFactory.close();
    }

    @Nested
    class RowsOfASegment {

        @Test
        void areExactlyTheRowsOfTheWorkflowsTheSegmentOwns() {
            // given
            var workflowIds = IntStream.range(0, 200).mapToObj(i -> "order-" + i).toList();
            store(workflowIds.stream().flatMap(JpaWorkflowIndexTest::startedAndWaiting).toList());

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
            store(workflowIds.stream().flatMap(JpaWorkflowIndexTest::startedAndWaiting).toList());
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
}
