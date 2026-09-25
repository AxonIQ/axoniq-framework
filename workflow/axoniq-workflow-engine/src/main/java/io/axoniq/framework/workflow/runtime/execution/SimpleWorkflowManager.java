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

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.query.utils.WorkflowStateQueryMatcher;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.manager.NonUniqueWorkflowInstanceMatchException;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowInstance;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowInstances;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.stream.Stream;

/**
 * Workflow Manager backed by the workflow history projection and live cancellation coordinators.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class SimpleWorkflowManager implements WorkflowManager {

    private final WorkflowHistoryRepository historyRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final WorkflowCancellationService cancellationService;
    private final WorkflowStore workflowStore;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Executor executor;

    /**
     * Creates a Workflow Manager using a history projection and live cancellation delivery.
     *
     * @param historyRepository   repository containing projected workflow states
     * @param executionRepository repository containing live workflow executions
     * @param cancellationService service delivering cancellation requests to live workflows
     * @param workflowStore       store the state of a non-live workflow is sourced from
     * @param unitOfWorkFactory   factory for the unit of work each such sourcing runs in
     * @param executor            executor used for asynchronous manager operations
     */
    public SimpleWorkflowManager(WorkflowHistoryRepository historyRepository,
                                 WorkflowExecutionRepository executionRepository,
                                 WorkflowCancellationService cancellationService,
                                 WorkflowStore workflowStore,
                                 UnitOfWorkFactory unitOfWorkFactory,
                                 Executor executor) {
        this.historyRepository = Objects.requireNonNull(historyRepository,
                                                        "The WorkflowHistoryRepository must not be null.");
        this.executionRepository = Objects.requireNonNull(executionRepository,
                                                          "The WorkflowExecutionRepository must not be null.");
        this.cancellationService = Objects.requireNonNull(cancellationService,
                                                          "The WorkflowCancellationService must not be null.");
        this.workflowStore = Objects.requireNonNull(workflowStore, "The WorkflowStore must not be null.");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory,
                                                        "The UnitOfWorkFactory must not be null.");
        this.executor = Objects.requireNonNull(executor, "The executor must not be null.");
    }

    private static Stream<String> explicitWorkflowIds(WorkflowStateQuery query) {
        return query.criteria().stream()
                    .filter(WorkflowStateQuery.WorkflowIdCriterion.class::isInstance)
                    .map(WorkflowStateQuery.WorkflowIdCriterion.class::cast)
                    .map(WorkflowStateQuery.WorkflowIdCriterion::workflowId);
    }

    private static <T> CompletableFuture<T> cancellation(CancellationOperation<T> operation) {
        try {
            return operation.request();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static <T> CompletableFuture<T> cancellation(CancellationOperation<T> operation,
                                                         @Nullable T unavailableResult) {
        try {
            return operation.request();
        } catch (NoSuchElementException e) {
            return CompletableFuture.completedFuture(unavailableResult);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public WorkflowInstances.Single findOne(WorkflowStateQuery query) {
        return new SingleInstanceResult(Objects.requireNonNull(query, "The WorkflowStateQuery must not be null."));
    }

    @Override
    public WorkflowInstances findMany(WorkflowStateQuery query) {
        return new MultipleInstances(Objects.requireNonNull(query, "The WorkflowStateQuery must not be null."));
    }

    /**
     * Resolves the states matching a query: live executions answer for themselves; every other candidate is sourced
     * from the {@link WorkflowStore}, so the answer for an id that is no longer live is the event store's, never a
     * projection that may still lag behind it.
     * <p>
     * Candidates for sourcing are the history matches that are not live, plus every workflow id the query names
     * explicitly, so a workflow the projection has not seen yet is still found by id. A sourced state is kept only when
     * it exists and still matches the query.
     */
    private CompletableFuture<List<WorkflowState>> matching(WorkflowStateQuery query) {
        return executionRepository.findAll(query)
                                  .thenCombine(historyRepository.findAll(query), (liveExecutions, history) -> {
                                      var byId = new LinkedHashMap<String, WorkflowState>();
                                      liveExecutions.forEach(e -> byId.putIfAbsent(e.state().workflowId(), e.state()));
                                      var candidates = new LinkedHashSet<String>();
                                      history.stream().map(WorkflowHistory::workflowId).forEach(candidates::add);
                                      explicitWorkflowIds(query).forEach(candidates::add);
                                      candidates.removeAll(byId.keySet());
                                      return new Merge(byId, List.copyOf(candidates));
                                  })
                                  .thenCompose(merge -> sourceAll(merge.candidates()).thenApply(sourced -> {
                                      for (WorkflowState state : sourced) {
                                          if (WorkflowStateQueryMatcher.matches(query, state)) {
                                              merge.byId().putIfAbsent(state.workflowId(), state);
                                          }
                                      }
                                      return List.copyOf(merge.byId().values());
                                  }));
    }

    private CompletableFuture<List<WorkflowState>> sourceAll(List<String> workflowIds) {
        var loads = workflowIds.stream().map(this::source).toList();
        return CompletableFuture.allOf(loads.toArray(CompletableFuture[]::new))
                                .thenApply(ignored -> loads.stream()
                                                           .map(CompletableFuture::resultNow)
                                                           .flatMap(Optional::stream)
                                                           .toList());
    }

    /**
     * Sources one workflow from the store in its own unit of work. Empty when the store holds no events for the id,
     * which is how an id a query names but nothing has recorded is told apart from an existing one.
     */
    private CompletableFuture<Optional<WorkflowState>> source(String workflowId) {
        return unitOfWorkFactory.create("workflow-manager-" + workflowId)
                                .executeWithResult(context -> workflowStore.findWorkflow(workflowId, context));
    }

    private CompletableFuture<List<WorkflowState>> matchingLive(WorkflowStateQuery query) {
        return executionRepository.findAll(query)
                                  .thenApply(executions -> executions.stream()
                                                                     .map(WorkflowExecution::state)
                                                                     .toList());
    }

    private CompletableFuture<Optional<WorkflowState>> singleMatch(WorkflowStateQuery query) {
        return matching(query).thenApply(matches -> {
            if (matches.size() > 1) {
                throw new NonUniqueWorkflowInstanceMatchException("The workflow instance query matched %d instances."
                                                                          .formatted(matches.size()));
            }
            return matches.isEmpty() ? Optional.empty() : Optional.of(matches.getFirst());
        });
    }

    @FunctionalInterface
    private interface CancellationOperation<T> {

        CompletableFuture<T> request();
    }

    private record Merge(LinkedHashMap<String, WorkflowState> byId, List<String> candidates) {

    }

    private final class SingleInstanceResult implements WorkflowInstances.Single {

        private final WorkflowStateQuery query;

        private SingleInstanceResult(WorkflowStateQuery query) {
            this.query = query;
        }

        @Override
        public CompletableFuture<@Nullable WorkflowState> singleState() {
            return singleMatch(query).thenApply(state -> state.map(DetachedWorkflowState::new).orElse(null));
        }

        @Override
        public Publisher<WorkflowInstance> instances() {
            return subscriber -> {
                @SuppressWarnings("resource")
                var publisher = new SubmissionPublisher<WorkflowInstance>(executor, Flow.defaultBufferSize());
                publisher.subscribe(FlowAdapters.toFlowSubscriber(subscriber));
                singleInstance().whenComplete((instance, error) -> {
                    if (error != null) {
                        publisher.closeExceptionally(error);
                    } else {
                        if (instance != null) {
                            publisher.submit(instance);
                        }
                        publisher.close();
                    }
                });
            };
        }

        @Override
        public CompletableFuture<Integer> size() {
            return singleMatch(query).thenApply(instance -> instance.isPresent() ? 1 : 0);
        }

        @Override
        public CompletableFuture<Boolean> requestStepCancellation(String stepName, @Nullable Throwable cause) {
            return singleInstance().thenCompose(instance -> instance == null
                    ? CompletableFuture.completedFuture(false)
                    : instance.requestStepCancellation(stepName, cause));
        }

        @Override
        public CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause) {
            return singleInstance().thenCompose(instance -> instance == null
                    ? CompletableFuture.completedFuture(0)
                    : instance.requestCancellationOfAllSteps(cause));
        }

        @Override
        public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
            return singleInstance().thenCompose(instance -> instance == null
                    ? CompletableFuture.completedFuture(null)
                    : instance.requestWorkflowCancellation(cause));
        }

        private CompletableFuture<@Nullable WorkflowInstance> singleInstance() {
            return singleMatch(query).thenApply(state -> state.map(ResolvedWorkflowInstance::new).orElse(null));
        }
    }

    private final class ResolvedWorkflowInstance implements WorkflowInstance {

        private final DetachedWorkflowState state;

        private ResolvedWorkflowInstance(WorkflowState state) {
            this.state = new DetachedWorkflowState(state);
        }

        @Override
        public CompletableFuture<WorkflowState> state() {
            return CompletableFuture.completedFuture(state);
        }

        @Override
        public CompletableFuture<Boolean> requestStepCancellation(String stepName, @Nullable Throwable cause) {
            return cancellation(() -> cancellationService.requestStepCancellation(state.workflowId(), stepName, cause),
                                false);
        }

        @Override
        public CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause) {
            return cancellation(() -> cancellationService.requestCancellationOfAllSteps(state.workflowId(), cause), 0);
        }

        @Override
        public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
            return cancellation(() -> cancellationService.requestWorkflowCancellation(state.workflowId(), cause), null);
        }
    }

    private final class MultipleInstances implements WorkflowInstances {

        private final WorkflowStateQuery query;

        private MultipleInstances(WorkflowStateQuery query) {
            this.query = query;
        }

        @Override
        public Publisher<WorkflowInstance> instances() {
            return subscriber -> {
                @SuppressWarnings("resource")
                var publisher = new SubmissionPublisher<WorkflowInstance>(executor, Flow.defaultBufferSize());
                publisher.subscribe(FlowAdapters.toFlowSubscriber(subscriber));
                matching(query).whenComplete((states, error) -> {
                    if (error != null) {
                        publisher.closeExceptionally(error);
                        return;
                    }
                    for (WorkflowState state : states) {
                        publisher.submit(new ResolvedWorkflowInstance(state));
                    }
                    publisher.close();
                });
            };
        }

        @Override
        public CompletableFuture<Integer> size() {
            return matching(query).thenApply(List::size);
        }

        @Override
        public CompletableFuture<Boolean> requestStepCancellation(String stepName, @Nullable Throwable cause) {
            return matchingLive(query).thenCompose(states -> {
                var operations = new ArrayList<CompletableFuture<Boolean>>();
                for (WorkflowState state : states) {
                    operations.add(cancellation(() -> cancellationService.requestStepCancellation(
                            state.workflowId(), stepName, cause
                    )));
                }
                return CompletableFuture.allOf(operations.toArray(CompletableFuture[]::new))
                                        .thenApply(ignored -> !operations.isEmpty());
            });
        }

        @Override
        public CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause) {
            return matchingLive(query).thenCompose(states -> {
                var operations = new ArrayList<CompletableFuture<Integer>>();
                for (WorkflowState state : states) {
                    operations.add(cancellation(() -> cancellationService.requestCancellationOfAllSteps(
                            state.workflowId(), cause
                    )));
                }
                return CompletableFuture.allOf(operations.toArray(CompletableFuture[]::new))
                                        .thenApply(ignored -> operations.stream()
                                                                        .mapToInt(CompletableFuture::resultNow)
                                                                        .sum());
            });
        }

        @Override
        public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
            return matchingLive(query).thenCompose(states -> {
                var operations = new ArrayList<CompletableFuture<Void>>();
                for (WorkflowState state : states) {
                    operations.add(cancellation(() -> cancellationService.requestWorkflowCancellation(
                            state.workflowId(), cause
                    )));
                }
                return CompletableFuture.allOf(operations.toArray(CompletableFuture[]::new));
            });
        }
    }
}
