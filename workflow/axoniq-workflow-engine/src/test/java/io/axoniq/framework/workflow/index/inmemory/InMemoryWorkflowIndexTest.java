/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.inmemory;

import io.axoniq.framework.workflow.runtime.execution.WaitAssociation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtSet;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowStarted;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexRows;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests when the {@link InMemoryWorkflowIndex} applies a stored change: with the commit of the context it is stored in.
 * Also tests what an initialization adds, and that a module loads nothing before it.
 *
 * @author Stefan Dragisic
 */
class InMemoryWorkflowIndexTest {

    private static final String MODULE = "orders";
    private static final WorkflowIndexChanges STARTED =
            new WorkflowIndexChanges(List.of(new WorkflowStarted("order-1")));

    private final UnitOfWorkFactory unitOfWorkFactory = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);
    private final InMemoryWorkflowIndex testSubject = new InMemoryWorkflowIndex();

    @BeforeEach
    void setUp() {
        testSubject.initialize(MODULE, Set.of(), List.of()).orTimeout(5, TimeUnit.SECONDS).join();
    }

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
    class Initialization {

        @Test
        void isWhatLetsAModuleLoadItsRows() {
            // given
            testSubject.store("payments", STARTED, null).orTimeout(5, TimeUnit.SECONDS).join();

            // when
            var beforeInitialization = load("payments");
            testSubject.initialize("payments", Set.of(), List.of()).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(beforeInitialization).isEmpty();
            assertThat(load("payments")).hasValueSatisfying(
                    rows -> assertThat(rows.runningWorkflowIds()).containsExactly("order-1")
            );
        }

        @Test
        void addsTheGivenWorkflowsAndTheirWaitsWhileKeepingTheStoredRows() {
            // given
            var wakesAt = Instant.parse("2026-12-24T18:00:00Z");
            testSubject.store("payments", new WorkflowIndexChanges(List.of(new WakesAtSet("order-1", wakesAt))), null)
                       .orTimeout(5, TimeUnit.SECONDS)
                       .join();

            // when
            testSubject.initialize("payments",
                                   Set.of("order-1", "order-2"),
                                   List.of(wait("order-2"), wait("order-3")))
                       .orTimeout(5, TimeUnit.SECONDS)
                       .join();

            // then: the wait of a workflow that does not run is left out
            var rows = load("payments").orElseThrow();
            assertThat(rows.runningWorkflowIds()).containsExactlyInAnyOrder("order-1", "order-2");
            assertThat(rows.wakesAt()).containsExactly(Map.entry("order-1", wakesAt));
            assertThat(rows.waits()).extracting(WaitAssociation::workflowId).containsExactly("order-2");
        }

        private static WaitAssociation wait(String workflowId) {
            return new WaitAssociation(workflowId, "awaitPaid", new QualifiedName("paid"), Set.of());
        }

        private Optional<WorkflowIndexRows> load(String module) {
            return testSubject.load(module, Segment.ROOT_SEGMENT, null).orTimeout(5, TimeUnit.SECONDS).join();
        }
    }

    private Set<String> runningWorkflowIds() {
        return testSubject.load(MODULE, Segment.ROOT_SEGMENT, null)
                          .orTimeout(5, TimeUnit.SECONDS)
                          .join()
                          .orElseThrow()
                          .runningWorkflowIds();
    }
}
