/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link InMemoryWorkflowExecutionRepository#findAllOwnedBy(Segment)} lists exactly the executions a
 * segment owns, for every shape of segment a processor may hold, and without inspecting the executions of other
 * segments.
 *
 * @author Allard Buijze
 */
class InMemoryWorkflowExecutionRepositorySegmentLookupTest {

    private static List<String> workflowIds(int count) {
        // Mixes short sequential ids with random UUIDs, so hashes cover both signs and all low bits.
        return IntStream.range(0, count)
                        .mapToObj(i -> i % 2 == 0 ? "workflow-" + i : UUID.randomUUID().toString())
                        .toList();
    }

    private static InMemoryWorkflowExecutionRepository repositoryWith(List<String> workflowIds) {
        var repository = new InMemoryWorkflowExecutionRepository();
        workflowIds.forEach(id -> repository.save(id, () -> new InspectionCountingWorkflowExecution(id)));
        return repository;
    }

    private static Set<String> idsOf(Set<WorkflowExecution> executions) {
        return executions.stream().map(WorkflowExecution::workflowId).collect(Collectors.toSet());
    }

    private static Set<String> idsMatching(List<String> workflowIds, Segment segment) {
        return workflowIds.stream().filter(segment::matches).collect(Collectors.toSet());
    }

    @Nested
    class FindAllOwnedBy {

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 3, 5, 16, 100, 1024, 1500, 5000})
        void listsExactlyTheExecutionsEachSegmentOfABalancedSplitMatches(int segmentCount) {
            // given: segment layouts with equal and unequal masks, some selecting more hash bits than the buckets
            var workflowIds = workflowIds(10_000);
            var repository = repositoryWith(workflowIds);
            var segments = Segment.splitBalanced(Segment.ROOT_SEGMENT, segmentCount - 1);

            // when
            var listedPerSegment = segments.stream().map(segment -> idsOf(repository.findAllOwnedBy(segment))).toList();

            // then
            for (int i = 0; i < segments.size(); i++) {
                assertThat(listedPerSegment.get(i))
                        .as("executions of segment %s", segments.get(i))
                        .isEqualTo(idsMatching(workflowIds, segments.get(i)));
            }
            assertThat(listedPerSegment.stream().mapToInt(Set::size).sum())
                    .as("every execution is listed by exactly one segment")
                    .isEqualTo(workflowIds.size());
        }

        @Test
        void listsTheExecutionsOfSplitAndMergedSegments() {
            // given
            var workflowIds = workflowIds(4_000);
            var repository = repositoryWith(workflowIds);
            var halves = Segment.ROOT_SEGMENT.split();
            var quarters = halves[0].split();
            var eighths = quarters[1].split();
            var merged = eighths[0].mergedWith(eighths[1]);

            // when / then
            var layouts = List.of(halves[0], halves[1], quarters[0], quarters[1], eighths[0], eighths[1], merged);
            for (var segment : layouts) {
                assertThat(idsOf(repository.findAllOwnedBy(segment)))
                        .as("executions of segment %s", segment)
                        .isEqualTo(idsMatching(workflowIds, segment));
            }
            assertThat(idsOf(repository.findAllOwnedBy(merged)))
                    .isEqualTo(idsOf(repository.findAllOwnedBy(quarters[1])));
        }

        @Test
        void rejectsAnExecutionWithAnEmptyIdThatNoSegmentOwns() {
            // given
            var repository = new InMemoryWorkflowExecutionRepository();

            // when / then
            assertThatThrownBy(() -> repository.save("", () -> new InspectionCountingWorkflowExecution("")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(repository.findAllOwnedBy(Segment.ROOT_SEGMENT.split()[0])).isEmpty();
            assertThat(repository.findAllOwnedBy(null)).isEmpty();
        }

        @Test
        void listsEveryExecutionWithoutSegment() {
            // given
            var workflowIds = workflowIds(100);
            var repository = repositoryWith(workflowIds);

            // when
            var listed = repository.findAllOwnedBy(null);

            // then: outside a segmented processor every execution is in scope
            assertThat(idsOf(listed)).containsExactlyInAnyOrderElementsOf(workflowIds);
        }

        @Test
        void followsExecutionsThatAreSavedAndRemoved() {
            // given
            var segment = new Segment(3, 15);
            var workflowIds = workflowIds(1_000);
            var repository = repositoryWith(workflowIds);
            var owned = new ArrayList<>(idsMatching(workflowIds, segment));
            var removed = owned.removeFirst();
            var replaced = owned.getFirst();
            var replacement = new InspectionCountingWorkflowExecution(replaced);

            // when
            repository.remove(removed, repository.findById(removed).orElseThrow());
            repository.remove(replaced, repository.findById(replaced).orElseThrow());
            repository.save(replaced, () -> replacement);

            // then
            var listed = repository.findAllOwnedBy(segment);
            assertThat(idsOf(listed)).containsExactlyInAnyOrderElementsOf(owned);
            assertThat(listed).contains(replacement);
        }

        @Test
        void inspectsNoExecutionOfAnotherSegment() {
            // given
            var segment = new Segment(5, 15);
            var executions = workflowIds(2_000).stream().map(InspectionCountingWorkflowExecution::new).toList();
            var repository = new InMemoryWorkflowExecutionRepository();
            executions.forEach(execution -> repository.save(execution.id(), () -> execution));
            var inspectionsBefore = executions.stream().collect(Collectors.toMap(
                    execution -> execution, InspectionCountingWorkflowExecution::inspections
            ));

            // when
            repository.findAllOwnedBy(segment);

            // then
            assertThat(executions.stream()
                                 .filter(execution -> !segment.matches(execution.id()))
                                 .filter(execution -> execution.inspections() != inspectionsBefore.get(execution)))
                    .as("executions of other segments inspected by the lookup")
                    .isEmpty();
        }

        @Test
        void listsOnlyOwnedExecutionsWhileOthersAreConcurrentlySavedAndRemoved() throws Exception {
            // given: stable executions, plus churn of executions saved and removed by other threads
            var segment = new Segment(6, 15);
            var stableIds = workflowIds(2_000);
            var repository = repositoryWith(stableIds);
            var expectedStable = idsMatching(stableIds, segment);
            var writers = 4;
            var start = new CountDownLatch(1);
            var survivors = ConcurrentHashMap.<String>newKeySet();
            var violations = ConcurrentHashMap.<String>newKeySet();
            ExecutorService executor = Executors.newFixedThreadPool(writers + 1);
            try {
                List<Future<?>> tasks = new ArrayList<>();
                for (int w = 0; w < writers; w++) {
                    int writer = w;
                    tasks.add(executor.submit(() -> {
                        await(start);
                        for (int i = 0; i < 5_000; i++) {
                            var id = "churn-" + writer + "-" + i;
                            var execution = repository.save(id, () -> new InspectionCountingWorkflowExecution(id));
                            if (i % 3 != 0) {
                                repository.remove(id, execution);
                            } else {
                                survivors.add(id);
                            }
                        }
                    }));
                }
                tasks.add(executor.submit(() -> {
                    await(start);
                    for (int i = 0; i < 200; i++) {
                        var listed = idsOf(repository.findAllOwnedBy(segment));
                        listed.stream().filter(Predicate.not(segment::matches)).forEach(violations::add);
                        if (!listed.containsAll(expectedStable)) {
                            violations.add("a stable execution was missing from lookup " + i);
                        }
                    }
                }));

                // when
                start.countDown();
                for (var task : tasks) {
                    task.get(30, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdownNow();
            }

            // then
            assertThat(violations).isEmpty();
            var expected = new HashSet<>(expectedStable);
            expected.addAll(idsMatching(List.copyOf(survivors), segment));
            assertThat(idsOf(repository.findAllOwnedBy(segment))).isEqualTo(expected);
        }

        private static void await(CountDownLatch latch) {
            try {
                assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }
}
