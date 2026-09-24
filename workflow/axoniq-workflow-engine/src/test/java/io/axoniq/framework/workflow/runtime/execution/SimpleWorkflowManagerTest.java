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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.api.WorkflowHistoryRepository;
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
import org.junit.jupiter.api.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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
        var executions = new InMemoryWorkflowExecutionRepository();
        var execution = execution("order-42", "PaymentWorkflow");
        executions.save("order-42", () -> execution);
        var cancellations = new WorkflowCancellationService();
        var cancellation = mock(WorkflowCancellation.class);
        cancellations.register("order-42", cancellation);
        when(cancellation.requestStepCancellation("reserve-funds", null))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(cancellation.requestCancellationOfAllSteps(null)).thenReturn(CompletableFuture.completedFuture(2));
        when(cancellation.requestWorkflowCancellation(null)).thenReturn(CompletableFuture.completedFuture(null));
        var manager = manager(history(), executions, cancellations);

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
        var executions = new InMemoryWorkflowExecutionRepository();
        executions.save("order-42", () -> execution("order-42", "PaymentWorkflow"));
        executions.save("order-43", () -> execution("order-43", "PaymentWorkflow"));
        var cancellations = new WorkflowCancellationService();
        var firstCancellation = cancellation(false, 2);
        var secondCancellation = cancellation(true, 3);
        cancellations.register("order-42", firstCancellation);
        cancellations.register("order-43", secondCancellation);
        var manager = manager(history(), executions, cancellations);

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

        projectedState.setStatus(WorkflowStatus.COMPLETED, null, false, event, StubProcessingContext.forMessage(event));

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
                                  historicalEvent,
                                  StubProcessingContext.forMessage(historicalEvent));
        var liveState = state("order-42", "PaymentWorkflow");
        var liveEvent = eventMessage("live-payload");
        liveState.setStatus(WorkflowStatus.STARTED,
                            null,
                            false,
                            liveEvent,
                            StubProcessingContext.forMessage(liveEvent));
        WorkflowExecution execution = mock(WorkflowExecution.class);
        when(execution.state()).thenReturn(liveState);
        var executions = new InMemoryWorkflowExecutionRepository();
        executions.save("order-42", () -> execution);
        var manager = manager(history(new WorkflowHistory("order-42", historicalState)), executions);

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

    private static SimpleWorkflowManager manager(WorkflowHistoryRepository history) {
        return manager(history, new InMemoryWorkflowExecutionRepository());
    }

    private static SimpleWorkflowManager manager(WorkflowHistoryRepository history,
                                                 WorkflowExecutionRepository executions) {
        return manager(history, executions, new WorkflowCancellationService());
    }

    private static SimpleWorkflowManager manager(WorkflowHistoryRepository history,
                                                 WorkflowExecutionRepository executions,
                                                 WorkflowCancellationService cancellations) {
        return new SimpleWorkflowManager(history,
                                         executions,
                                         cancellations,
                                         storeMirroring(history),
                                         new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE),
                                         Runnable::run);
    }

    /**
     * A store that answers what the history projection holds, and an empty state for anything else — the shape of a
     * store whose projection is fully caught up, so the tests keep judging the merge rules rather than the sourcing.
     */
    private static WorkflowStore storeMirroring(WorkflowHistoryRepository history) {
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
                return history.findById(workflowId).thenApply(entry -> entry.map(WorkflowHistory::state));
            }
        };
    }

    private static WorkflowExecution execution(String workflowId, String workflowName) {
        var execution = mock(WorkflowExecution.class);
        when(execution.state()).thenReturn(state(workflowId, workflowName));
        return execution;
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
