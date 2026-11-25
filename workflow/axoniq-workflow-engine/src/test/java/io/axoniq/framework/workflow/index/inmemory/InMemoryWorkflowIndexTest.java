/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.inmemory;

import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowStarted;
import io.axoniq.framework.workflow.runtime.execution.SegmentKeyRange;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests when the {@link InMemoryWorkflowIndex} applies a stored change: with the commit of the context it is stored in.
 *
 * @author Stefan Dragisic
 */
class InMemoryWorkflowIndexTest {

    private static final String MODULE = "orders";
    private static final WorkflowIndexChanges STARTED =
            new WorkflowIndexChanges(List.of(new WorkflowStarted("order-1")), null);

    private final UnitOfWorkFactory unitOfWorkFactory = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);
    private final InMemoryWorkflowIndex testSubject = new InMemoryWorkflowIndex();

    @Nested
    class StoredChange {

        @Test
        void isAppliedOnceItsContextCommits() {
            // when
            var seenBeforeCommit = unitOfWorkFactory.create().executeWithResult(
                    context -> testSubject.store(MODULE, STARTED, context).thenApply(stored -> runningWorkflowIds())
            ).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(seenBeforeCommit).isEmpty();
            assertThat(runningWorkflowIds()).containsExactly("order-1");
        }

        @Test
        void isNotAppliedWhenItsContextRollsBack() {
            // when
            var result = unitOfWorkFactory.create().executeWithResult(
                    context -> testSubject.store(MODULE, STARTED, context).thenCompose(
                            stored -> CompletableFuture.failedFuture(new IllegalStateException("The token is lost."))
                    )
            ).orTimeout(5, TimeUnit.SECONDS);

            // then
            assertThat(result).failsWithin(5, TimeUnit.SECONDS);
            assertThat(runningWorkflowIds()).isEmpty();
        }

        @Test
        void isAppliedAtOnceWithoutAContext() {
            // when
            testSubject.store(MODULE, STARTED, null).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(runningWorkflowIds()).containsExactly("order-1");
        }
    }

    @Nested
    class StoredPosition {

        @Test
        void isLoadedForExactlyItsSegment() {
            // given
            var halves = Segment.ROOT_SEGMENT.split();

            // when
            storePosition(halves[0], 42L);

            // then
            assertThat(positionOf(halves[0])).hasValue(42L);
            assertThat(positionOf(halves[1])).isEmpty();
            assertThat(positionOf(Segment.ROOT_SEGMENT)).isEmpty();
        }

        @Test
        void replacesThePositionsOfTheSegmentsAMergeOrASplitReplaced() {
            // given
            var halves = Segment.ROOT_SEGMENT.split();
            storePosition(halves[0], 42L);
            storePosition(halves[1], 43L);

            // when: the halves are merged
            storePosition(Segment.ROOT_SEGMENT, 44L);

            // then
            assertThat(positionOf(Segment.ROOT_SEGMENT)).hasValue(44L);
            assertThat(positionOf(halves[0])).isEmpty();
            assertThat(positionOf(halves[1])).isEmpty();

            // when: the root is split again
            storePosition(halves[0], 45L);

            // then
            assertThat(positionOf(halves[0])).hasValue(45L);
            assertThat(positionOf(Segment.ROOT_SEGMENT)).isEmpty();
        }

        private void storePosition(Segment segment, long position) {
            testSubject.store(MODULE,
                              new WorkflowIndexChanges(List.of(), new WorkflowIndexChanges.SegmentPosition(
                                      SegmentKeyRange.of(segment), position
                              )),
                              null)
                       .orTimeout(5, TimeUnit.SECONDS)
                       .join();
        }

        private java.util.OptionalLong positionOf(Segment segment) {
            return testSubject.load(MODULE, segment, null).orTimeout(5, TimeUnit.SECONDS).join().position();
        }
    }

    private Set<String> runningWorkflowIds() {
        return testSubject.load(MODULE, Segment.ROOT_SEGMENT, null)
                          .orTimeout(5, TimeUnit.SECONDS)
                          .join()
                          .runningWorkflowIds();
    }
}
