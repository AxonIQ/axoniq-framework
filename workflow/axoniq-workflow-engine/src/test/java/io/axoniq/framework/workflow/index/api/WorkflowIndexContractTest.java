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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The behaviour every {@link WorkflowIndex} that can be initialized shares: loading the running workflows a segment
 * owns once the index was initialized, storing deltas, and initializing it without removing entries.
 * <p>
 * Implementations extend this class and provide the index under test.
 *
 * @author Allard Buijze
 */
public abstract class WorkflowIndexContractTest {

    private static final String INDEX = "orders";
    private static final List<Segment> FOUR_SEGMENTS = IntStream.range(0, 4)
                                                                .mapToObj(id -> new Segment(id, 3))
                                                                .toList();

    private WorkflowIndex workflowIndex;

    /**
     * Returns the index under test, empty. Called once before every test.
     *
     * @return the index under test
     */
    protected abstract WorkflowIndex createIndex();

    @BeforeEach
    void setUpIndex() {
        workflowIndex = createIndex();
    }

    private Optional<Set<String>> load(String indexName, Segment segment) {
        return workflowIndex.load(indexName, segment).orTimeout(5, TimeUnit.SECONDS).join();
    }

    private Set<String> loadInitialized(String indexName, Segment segment) {
        return load(indexName, segment).orElseThrow();
    }

    private void store(String indexName, IndexDelta delta) {
        workflowIndex.store(indexName, delta).orTimeout(5, TimeUnit.SECONDS).join();
    }

    private void initialize(String indexName, Set<String> runningWorkflowIds) {
        workflowIndex.initialize(indexName, runningWorkflowIds).orTimeout(5, TimeUnit.SECONDS).join();
    }

    private static Segment anotherSegmentThan(Segment segment) {
        return FOUR_SEGMENTS.stream()
                            .filter(candidate -> candidate.getSegmentId() != segment.getSegmentId())
                            .findFirst()
                            .orElseThrow();
    }

    private static String anyIdOn(Segment segment) {
        return IntStream.range(0, 512)
                        .mapToObj(i -> "sharded-" + i)
                        .filter(segment::matches)
                        .findFirst()
                        .orElseThrow();
    }

    @Nested
    class Load {

        @Test
        void reportsNothingBeforeTheIndexIsInitialized() {
            // given - entries written by claims that could not initialize the index
            store(INDEX, new IndexDelta(Set.of("order-1"), Set.of()));

            // when
            var index = load(INDEX, Segment.ROOT_SEGMENT);

            // then - the entries may not cover the stored tokens, so the index vouches for nothing
            assertThat(index).isEmpty();
        }

        @Test
        void returnsOnlyTheRunningWorkflowsTheSegmentOwns() {
            // given
            var segment = FOUR_SEGMENTS.getFirst();
            var owned = anyIdOn(segment);
            var foreign = anyIdOn(anotherSegmentThan(segment));
            initialize(INDEX, Set.of(owned, foreign));

            // when
            var index = loadInitialized(INDEX, segment);

            // then
            assertThat(index).containsExactly(owned);
        }

        @Test
        void returnsTheRunningWorkflowsOwnedByEachSegmentOfAnySplit() {
            // given - ids, some of them with a negative hash
            var ids = IntStream.range(0, 300).mapToObj(i -> "workflow-" + i).collect(Collectors.toSet());
            assertThat(ids).anyMatch(id -> id.hashCode() < 0);
            initialize(INDEX, ids);

            for (int mask : new int[]{0, 1, 3, 7, 15}) {
                for (int segmentId = 0; segmentId <= mask; segmentId++) {
                    var segment = new Segment(segmentId, mask);

                    // when
                    var index = loadInitialized(INDEX, segment);

                    // then
                    assertThat(index)
                            .as("running workflows of %s", segment)
                            .isEqualTo(ids.stream().filter(segment::matches).collect(Collectors.toSet()));
                }
            }
        }

        @Test
        void returnsNothingForASegmentWithoutRunningWorkflows() {
            // given
            var segment = FOUR_SEGMENTS.getFirst();
            initialize(INDEX, Set.of(anyIdOn(segment)));

            // when
            var index = load(INDEX, anotherSegmentThan(segment));

            // then - initialized, and empty
            assertThat(index).hasValue(Set.of());
        }

        @Test
        void keepsIndicesWithDifferentNamesApart() {
            // given
            initialize(INDEX, Set.of("order-1"));
            store("invoices", new IndexDelta(Set.of("invoice-1"), Set.of()));

            // when
            var invoices = load("invoices", Segment.ROOT_SEGMENT);
            var orders = loadInitialized(INDEX, Segment.ROOT_SEGMENT);

            // then - each index is initialized on its own, and holds its own entries
            assertThat(invoices).isEmpty();
            assertThat(orders).containsExactly("order-1");
        }
    }

    @Nested
    class Store {

        @BeforeEach
        void initializeIndex() {
            initialize(INDEX, Set.of());
        }

        @Test
        void appliesTheDelta() {
            // given
            store(INDEX, new IndexDelta(Set.of("order-1", "order-2"), Set.of()));

            // when
            store(INDEX, new IndexDelta(Set.of("order-3"), Set.of("order-1")));

            // then
            assertThat(loadInitialized(INDEX, Segment.ROOT_SEGMENT)).containsExactlyInAnyOrder("order-2", "order-3");
        }

        @Test
        void acceptsAddingAStoredIdAndRemovingAnUnknownId() {
            // given
            store(INDEX, new IndexDelta(Set.of("order-1"), Set.of()));

            // when - a write re-applied after an earlier attempt reported failure, but stored anyway
            store(INDEX, new IndexDelta(Set.of("order-1"), Set.of("order-2")));

            // then
            assertThat(loadInitialized(INDEX, Segment.ROOT_SEGMENT)).containsExactly("order-1");
        }

        @Test
        void removesAndAddsManyIdsInOneWrite() {
            // given
            var first = IntStream.range(0, 1200).mapToObj(i -> "first-" + i).collect(Collectors.toSet());
            var second = IntStream.range(0, 1200).mapToObj(i -> "second-" + i).collect(Collectors.toSet());
            store(INDEX, new IndexDelta(first, Set.of()));

            // when
            store(INDEX, new IndexDelta(second, first));

            // then
            assertThat(loadInitialized(INDEX, Segment.ROOT_SEGMENT)).isEqualTo(second);
        }

        @Test
        void acceptsValidIdsUpToTheMaximumLength() {
            // given - the longest valid workflow id, and one with the first and last printable ASCII characters
            var longest = "w".repeat(255);
            var edgeCharacters = " order-1~";

            // when
            store(INDEX, new IndexDelta(Set.of(longest, edgeCharacters), Set.of()));

            // then
            assertThat(loadInitialized(INDEX, Segment.ROOT_SEGMENT)).containsExactlyInAnyOrder(longest, edgeCharacters);
        }
    }

    @Nested
    class Initialize {

        @Test
        void addsTheRunningWorkflowsAndMarksTheIndexInitialized() {
            // when
            initialize(INDEX, Set.of("order-1", "order-2"));

            // then
            assertThat(load(INDEX, Segment.ROOT_SEGMENT)).hasValue(Set.of("order-1", "order-2"));
        }

        @Test
        void neverRemovesAnEntry() {
            // given - an instance another claim wrote after this claim read the running workflows
            store(INDEX, new IndexDelta(Set.of("order-3"), Set.of()));

            // when
            initialize(INDEX, Set.of("order-1"));

            // then
            assertThat(loadInitialized(INDEX, Segment.ROOT_SEGMENT)).containsExactlyInAnyOrder("order-1", "order-3");
        }

        @Test
        void onlyAddsTheRunningWorkflowsWhenInitializedAgain() {
            // given
            initialize(INDEX, Set.of("order-1"));

            // when
            initialize(INDEX, Set.of("order-1", "order-2"));

            // then
            assertThat(loadInitialized(INDEX, Segment.ROOT_SEGMENT)).containsExactlyInAnyOrder("order-1", "order-2");
        }

        @Test
        void addsManyRunningWorkflowsAtOnce() {
            // given
            var ids = IntStream.range(0, 1200).mapToObj(i -> "order-" + i).collect(Collectors.toSet());

            // when
            initialize(INDEX, ids);

            // then
            assertThat(loadInitialized(INDEX, Segment.ROOT_SEGMENT)).isEqualTo(ids);
        }
    }
}
