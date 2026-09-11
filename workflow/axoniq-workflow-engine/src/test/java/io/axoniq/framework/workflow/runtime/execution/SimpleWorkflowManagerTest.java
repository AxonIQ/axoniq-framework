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
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.manager.NonUniqueWorkflowInstanceMatchException;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowInstance;
import org.axonframework.messaging.core.VersionedType;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SimpleWorkflowManagerTest {

    @Test
    void readsTheStateOfTheSingleHistoryEntryMatchingTheCriteria() {
        var history = history(
                new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow")),
                new WorkflowHistory("order-43", state("order-43", "ShippingWorkflow"))
        );
        var manager = manager(history);

        var instance = manager.findOne(WorkflowStateQuery.byWorkflowDefinitionId(
                                              VersionedType.of("PaymentWorkflow", "1.0")))
                              .single()
                              .join();
        assertThat(instance).isNotNull();
        var state = instance.state().join();

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
                                        .single()
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

        var instance = manager.findOne(WorkflowStateQuery.all().workflowId("order-42")).single().join();

        assertThat(instance.requestStepCancellation("step", null).join()).isFalse();
        assertThat(instance.requestCancellationOfAllSteps(null).join()).isZero();
        assertThat(instance.requestWorkflowCancellation(null).join()).isNull();
    }

    @Test
    void returnsAStateThatDoesNotChangeWhenTheProjectedStateChangesLater() {
        var projectedState = state("order-42", "PaymentWorkflow");
        var history = history(new WorkflowHistory("order-42", projectedState));
        var manager = manager(history);

        var detachedState = manager.findOne(WorkflowStateQuery.all().workflowId("order-42"))
                              .single()
                              .join()
                              .state()
                              .join();
        projectedState.setStatus(WorkflowStatus.COMPLETED, null, false);

        assertThat(detachedState.workflowStatus()).isEqualTo(WorkflowStatus.NONE);
        assertThat(projectedState.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
    }

    @Test
    void prefersTheMatchingLiveStateOverHistoryForTheSameWorkflowId() {
        var historicalState = state("order-42", "PaymentWorkflow");
        historicalState.setStatus(WorkflowStatus.COMPLETED, null, false);
        var liveState = state("order-42", "PaymentWorkflow");
        liveState.setStatus(WorkflowStatus.STARTED, null, false);
        WorkflowExecution execution = mock(WorkflowExecution.class);
        when(execution.state()).thenReturn(liveState);
        var executions = new InMemoryWorkflowExecutionRepository();
        executions.save("order-42", () -> execution);
        var manager = manager(history(new WorkflowHistory("order-42", historicalState)), executions);

        var detachedState = manager.findOne(WorkflowStateQuery.all().workflowId("order-42"))
                              .single()
                              .join()
                              .state()
                              .join();

        assertThat(detachedState.workflowStatus()).isEqualTo(WorkflowStatus.STARTED);
    }

    @Test
    void resolvesNoMatchingSingleInstanceAsNull() {
        var manager = manager(history());

        assertThat(manager.findOne(WorkflowStateQuery.all().workflowId("unknown")).single().join()).isNull();
    }

    @Test
    void exposesTheSingleResultAsAZeroOrOneInstanceCollection() {
        var manager = manager(history(new WorkflowHistory("order-42", state("order-42", "PaymentWorkflow"))));

        var result = manager.findOne(WorkflowStateQuery.all().workflowId("order-42"));

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
        return new SimpleWorkflowManager(history,
                                         executions,
                                         new WorkflowCancellationService(),
                                         Runnable::run);
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
}
