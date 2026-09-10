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
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.manager.NonUniqueWorkflowInstanceMatchException;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowInstance;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowInstances;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.Function;
import java.util.stream.Collectors;
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
    private final Executor executor;

    /**
     * Creates a Workflow Manager using a history projection and live cancellation delivery.
     *
     * @param historyRepository   repository containing projected workflow states
     * @param executionRepository repository containing live workflow executions
     * @param cancellationService service delivering cancellation requests to live workflows
     * @param executor            executor used for asynchronous manager operations
     */
    public SimpleWorkflowManager(WorkflowHistoryRepository historyRepository,
                                 WorkflowExecutionRepository executionRepository,
                                 WorkflowCancellationService cancellationService,
                                 Executor executor) {
        this.historyRepository = Objects.requireNonNull(historyRepository,
                                                        "The WorkflowHistoryRepository must not be null.");
        this.executionRepository = Objects.requireNonNull(executionRepository,
                                                          "The WorkflowExecutionRepository must not be null.");
        this.cancellationService = Objects.requireNonNull(cancellationService,
                                                          "The WorkflowCancellationService must not be null.");
        this.executor = Objects.requireNonNull(executor, "The executor must not be null.");
    }

    @Override
    public WorkflowInstances.Single findOne(WorkflowStateQuery query) {
        return new SingleInstanceResult(Objects.requireNonNull(query, "The WorkflowStateQuery must not be null."));
    }

    @Override
    public WorkflowInstances findMany(WorkflowStateQuery query) {
        return new MultipleInstances(Objects.requireNonNull(query, "The WorkflowStateQuery must not be null."));
    }

    private CompletableFuture<List<WorkflowState>> matching(WorkflowStateQuery query) {
        return CompletableFuture.supplyAsync(
                () -> Stream.concat(
                                    matchingLive(query).stream(),
                                    historyRepository.findAll(query).stream().map(WorkflowHistory::state)
                            )
                            .collect(Collectors.toMap(WorkflowState::workflowId,
                                                      Function.identity(),
                                                      (live, historical) -> live,
                                                      LinkedHashMap::new))
                            .values()
                            .stream()
                            .toList(),
                executor
        );
    }

    private List<WorkflowState> matchingLive(WorkflowStateQuery query) {
        return executionRepository.findAll(query).stream()
                                  .map(WorkflowExecution::state)
                                  .toList();
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

    private final class SingleInstanceResult implements WorkflowInstances.Single {

        private final WorkflowStateQuery query;

        private SingleInstanceResult(WorkflowStateQuery query) {
            this.query = query;
        }

        @Override
        public CompletableFuture<@Nullable WorkflowInstance> single() {
            return singleMatch(query).thenApply(state -> state.map(ResolvedWorkflowInstance::new).orElse(null));
        }

        @Override
        public Flow.Publisher<WorkflowInstance> instances() {
            return subscriber -> {
                @SuppressWarnings("resource")
                var publisher = new SubmissionPublisher<WorkflowInstance>(executor, Flow.defaultBufferSize());
                publisher.subscribe(subscriber);
                single().whenComplete((instance, error) -> {
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
            return single().thenCompose(instance -> instance == null
                    ? CompletableFuture.completedFuture(false)
                    : instance.requestStepCancellation(stepName, cause));
        }

        @Override
        public CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause) {
            return single().thenCompose(instance -> instance == null
                    ? CompletableFuture.completedFuture(0)
                    : instance.requestCancellationOfAllSteps(cause));
        }

        @Override
        public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
            return single().thenCompose(instance -> instance == null
                    ? CompletableFuture.completedFuture(null)
                    : instance.requestWorkflowCancellation(cause));
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
            return cancellation(() -> cancellationService.requestStepCancellation(state.workflowId(), stepName, cause));
        }

        @Override
        public CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause) {
            return cancellation(() -> cancellationService.requestCancellationOfAllSteps(state.workflowId(), cause));
        }

        @Override
        public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
            return cancellation(() -> cancellationService.requestWorkflowCancellation(state.workflowId(), cause));
        }
    }

    private final class MultipleInstances implements WorkflowInstances {

        private final WorkflowStateQuery query;

        private MultipleInstances(WorkflowStateQuery query) {
            this.query = query;
        }

        @Override
        public Flow.Publisher<WorkflowInstance> instances() {
            return subscriber -> {
                @SuppressWarnings("resource")
                var publisher = new SubmissionPublisher<WorkflowInstance>(executor, Flow.defaultBufferSize());
                publisher.subscribe(subscriber);
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
            return CompletableFuture.supplyAsync(() -> matchingLive(query), executor).thenCompose(states -> {
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
            return CompletableFuture.supplyAsync(() -> matchingLive(query), executor).thenCompose(states -> {
                var operations = new ArrayList<CompletableFuture<Integer>>();
                for (WorkflowState state : states) {
                    operations.add(cancellation(() -> cancellationService.requestCancellationOfAllSteps(
                            state.workflowId(), cause
                    )));
                }
                return CompletableFuture.allOf(operations.toArray(CompletableFuture[]::new))
                                        .thenApply(ignored -> {
                                            int cancelledSteps = 0;
                                            for (CompletableFuture<Integer> operation : operations) {
                                                cancelledSteps += operation.join();
                                            }
                                            return cancelledSteps;
                                        });
            });
        }

        @Override
        public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
            return CompletableFuture.supplyAsync(() -> matchingLive(query), executor).thenCompose(states -> {
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

    private static <T> CompletableFuture<T> cancellation(CancellationOperation<T> operation) {
        try {
            return operation.request();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @FunctionalInterface
    private interface CancellationOperation<T> {

        CompletableFuture<T> request();
    }
}
