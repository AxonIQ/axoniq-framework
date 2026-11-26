/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.query.utils.WorkflowStateQueryMatcher;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.manager.NonUniqueWorkflowInstanceMatchException;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowInstance;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class SimpleWorkflowManagerTest {

    @Test
    void readsTheStateOfTheSingleHistoryEntryMatchingTheCriteria() {
        var history = history(
                new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow")),
                new WorkflowHistory("order-43", state("order-43", "ShippingWorkflow"))
        );
        var manager = manager(history);

        var state = manager.findOne(WorkflowStateQuery.byWorkflowDefinitionId(
                                           VersionedType.of("PaymentWorkflow", "1.0")))
                           .singleState()
                           .join();
        assertThat(state).isNotNull();

        assertThat(state.workflowId()).isEqualTo("order-42");
        assertThat(state.payload()).containsEntry("orderId", "order-42");
    }

    @Test
    void failsTheSingleHandleWhenCriteriaMatchMoreThanOneHistoryEntry() {
        var history = history(
                new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow")),
                new WorkflowHistory("order-43", state("order-43", "PaymentWorkflow"))
        );
        var manager = manager(history);

        assertThatThrownBy(() -> manager.findOne(WorkflowStateQuery.byWorkflowDefinitionId(
                                                                   VersionedType.of("PaymentWorkflow", "1.0")
                                                           ))
                                        .singleState()
                                        .join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(NonUniqueWorkflowInstanceMatchException.class);
    }

    @Test
    void countsEveryHistoryEntryMatchingTheCriteria() {
        var history = history(
                new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow")),
                new WorkflowHistory("order-43", state("order-43", "PaymentWorkflow")),
                new WorkflowHistory("order-44", state("order-44", "ShippingWorkflow"))
        );
        var manager = manager(history);

        var count = manager.findMany(WorkflowStateQuery.byWorkflowDefinitionId(
                                           VersionedType.of("PaymentWorkflow", "1.0")))
                           .size()
                           .join();

        assertThat(count).isEqualTo(2);
    }

    @Test
    void ignoresCancellationOfAHistoryOnlyWorkflow() {
        var history = history(new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow")));
        var manager = manager(history);

        var instance = manager.findOne(WorkflowStateQuery.all().workflowId("order-42"));

        assertThat(instance.requestStepCancellation("step", null).join()).isFalse();
        assertThat(instance.requestCancellationOfAllSteps(null).join()).isZero();
        assertThat(instance.requestWorkflowCancellation(null).join()).isNull();
    }

    @Test
    void forwardsCancellationRequestsForALiveSingleInstance() {
        var liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
        var cancellation = mock(WorkflowCancellation.class);
        enterCancellable(liveWorkflows, execution("order-42", "PaymentWorkflow", cancellation));
        when(cancellation.requestStepCancellation("reserve-funds", null))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(cancellation.requestCancellationOfAllSteps(null)).thenReturn(CompletableFuture.completedFuture(2));
        when(cancellation.requestWorkflowCancellation(null)).thenReturn(CompletableFuture.completedFuture(null));
        var manager = manager(history(), liveWorkflows);

        var instance = manager.findOne(WorkflowStateQuery.byWorkflowId("order-42"));

        assertThat(instance.requestStepCancellation("reserve-funds", null).join()).isTrue();
        assertThat(instance.requestCancellationOfAllSteps(null).join()).isEqualTo(2);
        assertThat(instance.requestWorkflowCancellation(null)).isCompleted();
        verify(cancellation).requestStepCancellation("reserve-funds", null);
        verify(cancellation).requestCancellationOfAllSteps(null);
        verify(cancellation).requestWorkflowCancellation(null);
    }

    @Test
    void aggregatesCancellationRequestsForEveryMatchingLiveInstance() {
        var liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
        var firstCancellation = cancellation(false, 2);
        var secondCancellation = cancellation(true, 3);
        enterCancellable(liveWorkflows, execution("order-42", "PaymentWorkflow", firstCancellation));
        enterCancellable(liveWorkflows, execution("order-43", "PaymentWorkflow", secondCancellation));
        var manager = manager(history(), liveWorkflows);

        var instances = manager.findMany(WorkflowStateQuery.byWorkflowDefinitionId(
                VersionedType.of("PaymentWorkflow", "1.0")
        ));

        assertThat(instances.requestStepCancellation("reserve-funds", null).join()).isTrue();
        assertThat(instances.requestCancellationOfAllSteps(null).join()).isEqualTo(5);
        assertThat(instances.requestWorkflowCancellation(null)).isCompleted();
        verify(firstCancellation).requestStepCancellation("reserve-funds", null);
        verify(secondCancellation).requestStepCancellation("reserve-funds", null);
        verify(firstCancellation).requestCancellationOfAllSteps(null);
        verify(secondCancellation).requestCancellationOfAllSteps(null);
        verify(firstCancellation).requestWorkflowCancellation(null);
        verify(secondCancellation).requestWorkflowCancellation(null);
    }

    @Test
    void returnsAStateThatDoesNotChangeWhenTheProjectedStateChangesLater() {
        var projectedState = state("order-42", "PaymentWorkflow");
        var history = history(new WorkflowHistory("order-42", projectedState));
        var manager = manager(history);

        var detachedState = manager.findOne(WorkflowStateQuery.all().workflowId("order-42"))
                                   .singleState()
                                   .join();
        var event = eventMessage("payload");

        projectedState.setStatus(WorkflowStatus.COMPLETED, null, false, StubProcessingContext.forMessage(event));

        assertThat(detachedState.workflowStatus()).isEqualTo(WorkflowStatus.NONE);
        assertThat(projectedState.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
    }

    @Test
    void prefersTheMatchingLiveStateOverHistoryForTheSameWorkflowId() {
        var historicalState = state("order-42", "PaymentWorkflow");
        var historicalEvent = eventMessage("historical-payload");
        historicalState.setStatus(WorkflowStatus.COMPLETED,
                                  null,
                                  false,
                                  StubProcessingContext.forMessage(historicalEvent));
        var liveState = state("order-42", "PaymentWorkflow");
        var liveEvent = eventMessage("live-payload");
        liveState.setStatus(WorkflowStatus.STARTED,
                            null,
                            false,
                            StubProcessingContext.forMessage(liveEvent));
        var execution = new InspectionCountingWorkflowExecution("order-42", liveState);
        var liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
        liveWorkflows.enter("order-42", false, () -> execution);
        var manager = manager(history(new WorkflowHistory("order-42", historicalState)), liveWorkflows);

        var detachedState = manager.findOne(WorkflowStateQuery.all().workflowId("order-42"))
                                   .singleState()
                                   .join();

        assertThat(detachedState.workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
    }

    @Test
    void resolvesNoMatchingSingleInstanceAsNull() {
        var manager = manager(history());

        assertThat(manager.findOne(WorkflowStateQuery.all().workflowId("unknown")).singleState().join()).isNull();
    }

    @Test
    void exposesTheSingleStateAndZeroOrOneInstanceCollection() {
        var manager = manager(history(new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow"))));

        var result = manager.findOne(WorkflowStateQuery.all().workflowId("order-42"));

        assertThat(result.singleState().join().workflowId()).isEqualTo("order-42");
        assertThat(result.size().join()).isOne();
        assertThat(first(result.instances()).join().state().join().workflowId()).isEqualTo("order-42");
    }

    @Test
    void publishesResolvedInstancesWithNonOptionalState() {
        var manager = manager(history(new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow"))));

        var instance = first(manager.findMany(WorkflowStateQuery.all()).instances()).join();

        assertThat(instance.state().join().workflowId()).isEqualTo("order-42");
    }

    @Test
    void failsAQueryWhoseHistoryLookupFails() {
        // given - a history whose lookup fails
        var failingHistory = new WorkflowHistoryRepository() {
            @Override
            public CompletableFuture<List<WorkflowHistory>> findAll(WorkflowStateQuery query) {
                return CompletableFuture.failedFuture(new IllegalStateException("history unavailable"));
            }

            @Override
            public CompletableFuture<Optional<WorkflowHistory>> findById(String workflowId) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
        };
        var manager = manager(failingHistory);

        // when
        var size = manager.findMany(WorkflowStateQuery.all()).size();

        // then
        assertThat(size).failsWithin(Duration.ofSeconds(5))
                        .withThrowableThat()
                        .withRootCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void failsEveryOperationOfAQueryWhoseLookupThrows() {
        // given - a store of the executions in memory whose listing throws, rather than returning a failed future
        var throwing = new InMemoryWorkflowExecutionRepository() {
            @Override
            public CompletableFuture<Set<WorkflowExecution>> findAll(WorkflowStateQuery query) {
                throw new IllegalStateException("executions unavailable");
            }
        };
        var instances = manager(history(), new LiveWorkflows(throwing)).findMany(WorkflowStateQuery.all());

        // when
        var size = instances.size();
        var cancellation = instances.requestWorkflowCancellation(null);
        var published = first(instances.instances());

        // then - each fails, and a subscriber of the instances is told so
        assertThat(size).failsWithin(Duration.ofSeconds(5))
                        .withThrowableThat()
                        .withRootCauseInstanceOf(IllegalStateException.class);
        assertThat(cancellation).failsWithin(Duration.ofSeconds(5))
                                .withThrowableThat()
                                .withRootCauseInstanceOf(IllegalStateException.class);
        assertThat(published).failsWithin(Duration.ofSeconds(5))
                             .withThrowableThat()
                             .withRootCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void asksTheHistoryForTheLiveWorkflowsOnlyWhenTheEngineMayHibernate() {
        // given - a projected history that counts the queries it answers
        var queries = new AtomicInteger();
        var entries = history(new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow")));
        var countingHistory = new WorkflowHistoryRepository() {
            @Override
            public CompletableFuture<List<WorkflowHistory>> findAll(WorkflowStateQuery query) {
                queries.incrementAndGet();
                return entries.findAll(query);
            }

            @Override
            public CompletableFuture<Optional<WorkflowHistory>> findById(String workflowId) {
                return entries.findById(workflowId);
            }
        };
        var liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
        var store = storeMirroring(entries, new CopyOnWriteArrayList<>());
        var withoutHibernation = manager(countingHistory, liveWorkflows, store, () -> false, true);
        var withHibernation = manager(countingHistory, liveWorkflows, store, () -> true, true);
        var query = WorkflowStateQuery.byWorkflowDefinitionId(VersionedType.of("PaymentWorkflow", "1.0"));

        // when - without hibernation, every live workflow is in memory
        var cancelledWithout = withoutHibernation.findMany(query).requestWorkflowCancellation(null);

        // then
        assertThat(cancelledWithout).succeedsWithin(Duration.ofSeconds(5));
        assertThat(queries).hasValue(0);

        // when - with hibernation, the history finds the hibernated ones
        var cancelledWith = withHibernation.findMany(query).requestWorkflowCancellation(null);

        // then
        assertThat(cancelledWith).succeedsWithin(Duration.ofSeconds(5));
        assertThat(queries).hasValue(1);
    }

    /**
     * A hibernated workflow is live, but has no execution in memory: a query sources its state from the store.
     */
    @Nested
    class HibernatedWorkflow {

        private final LiveWorkflows liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
        private final List<String> sourced = new CopyOnWriteArrayList<>();
        private final SegmentClaim claim = claimOfEverySegment();
        private final WorkflowEngineHibernationSupport support = new WorkflowEngineHibernationSupport(
                new ControllableWorkflowScheduler(),
                Clock.systemUTC(),
                liveWorkflows,
                new RunningWorkflowsProjection(WorkflowIndex.none(), "orders"),
                (workflowId, resources, executionContext) -> {
                    throw new UnsupportedOperationException("A query does not wake a workflow.");
                },
                new WorkflowStarter(liveWorkflows),
                Duration.ofMinutes(1),
                Duration.ofSeconds(10),
                Runnable::run
        );

        private static SegmentClaim claimOfEverySegment() {
            var segmentClaims = new SegmentClaims();
            segmentClaims.initialize(null);
            return segmentClaims.startClaim(Segment.ROOT_SEGMENT, new StubProcessingContext());
        }

        /**
         * Leaves the given workflow hibernated, as a claim of its segment does with one it finds a hint of.
         */
        private void hibernated(String workflowId) {
            var hint = new HibernationHint(Instant.now().plus(Duration.ofHours(1)), List.of());
            var hibernated = support.hibernationOf(claim, workflowId, hint, CompletableFuture.completedFuture(null));
            try (var ignored = liveWorkflows.lock(workflowId)) {
                assertThat(liveWorkflows.adopt(hibernated)).isTrue();
            }
        }

        /**
         * Returns a manager whose store holds the given durable states, and records the workflows it sources.
         */
        private SimpleWorkflowManager manager(WorkflowHistoryRepository history,
                                              WorkflowHistoryRepository durable,
                                              boolean projectedHistory) {
            return SimpleWorkflowManagerTest.manager(history,
                                                     liveWorkflows,
                                                     storeMirroring(durable, sourced),
                                                     () -> true,
                                                     projectedHistory);
        }

        @Test
        void isSourcedWhenAQueryNamesItById() {
            // given - a history that does not hold it yet
            var durable = history(new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow")));
            hibernated("order-42");
            var manager = manager(history(), durable, true);

            // when
            var state = manager.findOne(WorkflowStateQuery.byWorkflowId("order-42")).singleState();

            // then
            assertThat(state).succeedsWithin(Duration.ofSeconds(5))
                             .extracting(WorkflowState::workflowId)
                             .isEqualTo("order-42");
            assertThat(sourced).containsExactly("order-42");
        }

        @Test
        void isCountedAmongTheWorkflowsInMemoryByAQueryWithoutHistory() {
            // given
            var durable = history(new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow")),
                                  new WorkflowHistory("order-43", state("order-43", "PaymentWorkflow")),
                                  new WorkflowHistory("order-44", state("order-44", "ShippingWorkflow")));
            hibernated("order-42");
            hibernated("order-43");
            hibernated("order-44");
            var inMemory = new InspectionCountingWorkflowExecution("order-45", state("order-45", "PaymentWorkflow"));
            liveWorkflows.enter("order-45", false, () -> inMemory);
            var manager = manager(history(), durable, false);

            // when
            var count = manager.findMany(WorkflowStateQuery.byWorkflowDefinitionId(
                    VersionedType.of("PaymentWorkflow", "1.0")
            )).size();

            // then - the workflow in memory answers for itself
            assertThat(count).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(3);
            assertThat(sourced).containsExactlyInAnyOrder("order-42", "order-43", "order-44");
        }

        @Test
        void isFoundByAQueryNamingNoIdOnlyOnceTheHistoryHoldsIt() {
            // given - a history whose projection did not see the start of order-43 yet
            var durable = history(new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow")),
                                  new WorkflowHistory("order-43", state("order-43", "PaymentWorkflow")));
            hibernated("order-42");
            hibernated("order-43");
            var manager = manager(history(new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow"))),
                                  durable,
                                  true);

            // when
            var count = manager.findMany(WorkflowStateQuery.byWorkflowDefinitionId(
                    VersionedType.of("PaymentWorkflow", "1.0")
            )).size();

            // then - the query sources only the hibernated workflows the history finds
            assertThat(count).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(1);
            assertThat(sourced).containsExactly("order-42");
        }
    }

    private static SimpleWorkflowManager manager(WorkflowHistoryRepository history) {
        return manager(history, new LiveWorkflows(new InMemoryWorkflowExecutionRepository()));
    }

    private static SimpleWorkflowManager manager(WorkflowHistoryRepository history, LiveWorkflows liveWorkflows) {
        return manager(history, liveWorkflows, storeMirroring(history, new CopyOnWriteArrayList<>()), () -> true, true);
    }

    private static SimpleWorkflowManager manager(WorkflowHistoryRepository history,
                                                 LiveWorkflows liveWorkflows,
                                                 WorkflowStore store,
                                                 BooleanSupplier hibernationEnabled,
                                                 boolean projectedHistory) {
        return new SimpleWorkflowManager(history,
                                         liveWorkflows,
                                         new WorkflowCancellationService(liveWorkflows, () -> NoHibernation.INSTANCE),
                                         store,
                                         new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE),
                                         Runnable::run,
                                         hibernationEnabled,
                                         projectedHistory);
    }

    /**
     * A store that answers what the history projection holds, and an empty state for anything else — the shape of a
     * store whose projection is fully caught up, so the tests keep judging the merge rules rather than the sourcing.
     * Records the workflows it sources in the given list.
     */
    private static WorkflowStore storeMirroring(WorkflowHistoryRepository history, List<String> sourced) {
        return new WorkflowStore() {
            @Override
            public CompletableFuture<RunningWorkflows> loadRunningWorkflows(ProcessingContext processingContext) {
                throw new UnsupportedOperationException("not used by the manager");
            }

            @Override
            public CompletableFuture<WorkflowState> loadWorkflow(String workflowId,
                                                                 ProcessingContext processingContext) {
                return findWorkflow(workflowId, processingContext).thenApply(state -> state.orElseGet(
                        () -> new EventSourcedWorkflowState(workflowId, VersionedType.of("none", "0.0.1"))));
            }

            @Override
            public CompletableFuture<Optional<WorkflowState>> findWorkflow(String workflowId,
                                                                           ProcessingContext processingContext) {
                sourced.add(workflowId);
                return history.findById(workflowId).thenApply(entry -> entry.map(WorkflowHistory::state));
            }
        };
    }

    private static WorkflowExecution execution(String workflowId,
                                               String workflowName,
                                               WorkflowCancellation cancellation) {
        var execution = mock(WorkflowExecutionFixture.CancellationCapableExecution.class);
        when(execution.workflowId()).thenReturn(workflowId);
        when(execution.state()).thenReturn(state(workflowId, workflowName));
        when(execution.workflowCancellation()).thenReturn(cancellation);
        return execution;
    }

    /**
     * Brings the given execution into memory through the given live workflows, taking cancellation requests, as the
     * engine does with an execution it starts.
     */
    private static void enterCancellable(LiveWorkflows liveWorkflows, WorkflowExecution execution) {
        liveWorkflows.enter(execution.workflowId(), false, () -> execution);
        liveWorkflows.markCancellable(execution);
    }

    private static WorkflowCancellation cancellation(boolean stepCancellationResult, int cancelledSteps) {
        var cancellation = mock(WorkflowCancellation.class);
        when(cancellation.requestStepCancellation("reserve-funds", null))
                .thenReturn(CompletableFuture.completedFuture(stepCancellationResult));
        when(cancellation.requestCancellationOfAllSteps(null))
                .thenReturn(CompletableFuture.completedFuture(cancelledSteps));
        when(cancellation.requestWorkflowCancellation(null)).thenReturn(CompletableFuture.completedFuture(null));
        return cancellation;
    }

    private static WorkflowHistoryRepository history(WorkflowHistory... histories) {
        var entries = List.of(histories);
        return new WorkflowHistoryRepository() {
            @Override
            public CompletableFuture<List<WorkflowHistory>> findAll(
                    WorkflowStateQuery query
            ) {
                return CompletableFuture.completedFuture(entries.stream()
                                                                 .filter(history -> WorkflowStateQueryMatcher.matches(
                                                                         query, history.state()
                                                                 ))
                                                                 .toList());
            }

            @Override
            public CompletableFuture<Optional<WorkflowHistory>> findById(String workflowId) {
                for (WorkflowHistory history : entries) {
                    if (history.workflowId().equals(workflowId)) {
                        return CompletableFuture.completedFuture(Optional.of(history));
                    }
                }
                return CompletableFuture.completedFuture(Optional.empty());
            }
        };
    }

    private static CompletableFuture<WorkflowInstance> first(
            Publisher<WorkflowInstance> publisher
    ) {
        var result = new CompletableFuture<WorkflowInstance>();
        publisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void onNext(WorkflowInstance instance) {
                result.complete(instance);
            }

            @Override
            public void onError(Throwable throwable) {
                result.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                if (!result.isDone()) {
                    result.completeExceptionally(new IllegalStateException("Expected a workflow instance."));
                }
            }
        });
        return result;
    }

    private static EventSourcedWorkflowState state(String workflowId, String workflowName) {
        return new EventSourcedWorkflowState(
                workflowId,
                Map.of("orderId", workflowId),
                VersionedType.of(workflowName, "1.0")
        );
    }

    private static GenericEventMessage eventMessage(String payload) {
        return new GenericEventMessage(MessageType.fromString("my.workflow.Event#1.0.0"), payload);
    }
}
