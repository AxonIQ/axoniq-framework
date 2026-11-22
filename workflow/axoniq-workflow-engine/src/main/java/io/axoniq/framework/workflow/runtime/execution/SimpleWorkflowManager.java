/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.query.utils.WorkflowStateQueryMatcher;
import io.axoniq.framework.workflow.runtime.api.manager.NonUniqueWorkflowInstanceMatchException;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowInstance;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowInstances;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import org.axonframework.common.Registration;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Workflow Manager backed by the workflow history projection and live cancellation coordinators.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class SimpleWorkflowManager implements WorkflowManager {

    /**
     * The number of workflows a query sources from the {@link WorkflowStore} at the same time.
     */
    static final int MAX_CONCURRENT_SOURCING = 16;
    private final WorkflowHistoryRepository historyRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final WorkflowCancellationService cancellationService;
    private final WorkflowStore workflowStore;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Executor executor;
    private final Supplier<Set<String>> hibernatedWorkflowIds;
    private final Function<Consumer<String>, Registration> wakeListeners;
    private final boolean projectedHistory;

    /**
     * Creates a Workflow Manager using a history projection and live cancellation delivery, for an engine that may
     * hibernate workflows.
     * <p>
     * A hibernated workflow is live, but has no execution in memory: its state is sourced from the
     * {@link WorkflowStore}, and a cancellation request wakes it. A query sources only the hibernated workflows it may
     * match: those it names, when it names workflow ids; otherwise those the history finds for it, or, when no history
     * is projected, every hibernated workflow. A query also learns of the workflows that wake while it looks them up,
     * so it does not miss a workflow that wakes or hibernates while it lists the workflows.
     *
     * @param historyRepository     repository containing projected workflow states
     * @param executionRepository   repository containing live workflow executions
     * @param cancellationService   service delivering cancellation requests to live workflows
     * @param workflowStore         store the state of a workflow that is not in memory is sourced from
     * @param unitOfWorkFactory     factory for the unit of work each such sourcing runs in
     * @param executor              executor used for asynchronous manager operations
     * @param hibernatedWorkflowIds supplies the identifiers of the workflows the engine hibernated at the moment it is
     *                              called, which a query naming workflow ids or finding them in the history only
     *                              checks those ids against
     * @param wakeListeners         registers a listener that the engine tells the identifier of every workflow that
     *                              wakes, and returns the registration that removes it again; a query registers one
     *                              for as long as it looks up the hibernated workflows
     * @param projectedHistory      whether the {@code historyRepository} is projected from the event store, so it
     *                              finds the hibernated workflows a query matches, {@code false} when the module
     *                              projects no history
     */
    public SimpleWorkflowManager(WorkflowHistoryRepository historyRepository,
                                 WorkflowExecutionRepository executionRepository,
                                 WorkflowCancellationService cancellationService,
                                 WorkflowStore workflowStore,
                                 UnitOfWorkFactory unitOfWorkFactory,
                                 Executor executor,
                                 Supplier<Set<String>> hibernatedWorkflowIds,
                                 Function<Consumer<String>, Registration> wakeListeners,
                                 boolean projectedHistory) {
        this.hibernatedWorkflowIds = Objects.requireNonNull(hibernatedWorkflowIds,
                                                            "The hibernated workflow ids must not be null.");
        this.wakeListeners = Objects.requireNonNull(wakeListeners, "The wake listeners must not be null.");
        this.projectedHistory = projectedHistory;
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
     * Candidates for sourcing are the history matches that are not in memory, plus every workflow id the query names
     * explicitly, so a workflow the projection has not seen yet is still found by id. Hibernated workflows are among
     * those, unless no history is projected and the query names no workflow id: then every hibernated workflow is a
     * candidate. A workflow that wakes while the query looks them up answers for itself while it is still in memory,
     * and is a candidate otherwise. A sourced state is kept only when it exists and still matches the query.
     */
    private CompletableFuture<List<WorkflowState>> matching(WorkflowStateQuery query) {
        var named = explicitWorkflowIds(query).toList();
        if (!named.isEmpty() || projectedHistory) {
            return merged(query, named, false).thenCompose(merge -> resolve(query, merge, Set.of(), named, false));
        }
        var woken = ConcurrentHashMap.<String>newKeySet();
        return whileListeningForWakes(woken::add, () -> merged(query, named, true))
                .thenCompose(merge -> resolve(query, merge, woken, named, false));
    }

    /**
     * Lists the executions in memory matching the query, and the candidates for sourcing: the history matches and the
     * named ids, plus, when {@code everyHibernated}, the hibernated workflows, read before and after the executions in
     * memory are listed, see {@link #hibernatedLookup(WorkflowStateQuery, List)}.
     */
    private CompletableFuture<Merge> merged(WorkflowStateQuery query, List<String> named, boolean everyHibernated) {
        var hibernatedBefore = everyHibernated ? Set.copyOf(hibernatedWorkflowIds.get()) : Set.<String>of();
        return executionRepository.findAll(query)
                                  .thenCombine(historyRepository.findAll(query), (liveExecutions, history) -> {
                                      var byId = new LinkedHashMap<String, WorkflowState>();
                                      liveExecutions.forEach(e -> byId.putIfAbsent(e.state().workflowId(), e.state()));
                                      var candidates = new LinkedHashSet<String>();
                                      history.stream().map(WorkflowHistory::workflowId).forEach(candidates::add);
                                      candidates.addAll(named);
                                      if (everyHibernated) {
                                          candidates.addAll(hibernatedBefore);
                                          candidates.addAll(hibernatedWorkflowIds.get());
                                      }
                                      candidates.removeAll(byId.keySet());
                                      return new Merge(byId, candidates);
                                  });
    }

    /**
     * Completes the given merge into the states matching the query. A workflow that woke while the query looked up the
     * hibernated workflows, and that the query names when it names workflow ids, answers for itself while it is still
     * in memory, and is sourced like the other candidates otherwise. A state of such a workflow or of a sourced one is
     * kept only when it matches the query and, for {@code live} results, is not terminal.
     */
    private CompletableFuture<List<WorkflowState>> resolve(WorkflowStateQuery query,
                                                           Merge merge,
                                                           Set<String> woken,
                                                           List<String> named,
                                                           boolean live) {
        Predicate<WorkflowState> kept = state -> WorkflowStateQueryMatcher.matches(query, state)
                && !(live && state.workflowStatus().isTerminal());
        var byId = merge.byId();
        var candidates = merge.candidates();
        for (String workflowId : woken) {
            if (byId.containsKey(workflowId) || candidates.contains(workflowId)
                    || !(named.isEmpty() || named.contains(workflowId))) {
                continue;
            }
            executionRepository.findById(workflowId).ifPresentOrElse(execution -> {
                var state = execution.state();
                if (kept.test(state)) {
                    byId.put(workflowId, state);
                }
            }, () -> candidates.add(workflowId));
        }
        return sourceAll(List.copyOf(candidates)).thenApply(sourced -> {
            for (WorkflowState state : sourced) {
                if (kept.test(state)) {
                    byId.putIfAbsent(state.workflowId(), state);
                }
            }
            return List.copyOf(byId.values());
        });
    }

    /**
     * Runs the given lookups while the given listener learns of every workflow that wakes, and removes the listener
     * once they completed, normally or exceptionally, or failed to start. Any failure to register the listener or to
     * start the lookups, {@link Error Errors} included, surfaces as a failed future. When removing the listener after
     * the lookups failed to start fails as well, that failure is added as suppressed to the failure of the lookups.
     */
    private <T> CompletableFuture<T> whileListeningForWakes(Consumer<String> listener,
                                                            Supplier<CompletableFuture<T>> lookups) {
        Registration registration;
        try {
            registration = wakeListeners.apply(listener);
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<T> result;
        try {
            result = lookups.get();
        } catch (Throwable e) {
            try {
                registration.cancel();
            } catch (Throwable cancelFailure) {
                e.addSuppressed(cancelFailure);
            }
            return CompletableFuture.failedFuture(e);
        }
        return result.whenComplete((ignored, error) -> registration.cancel());
    }

    /**
     * Sources the given workflows, at most {@link #MAX_CONCURRENT_SOURCING} at the same time.
     */
    private CompletableFuture<List<WorkflowState>> sourceAll(List<String> workflowIds) {
        var sourced = new ArrayList<WorkflowState>();
        var result = CompletableFuture.<Void>completedFuture(null);
        for (int from = 0; from < workflowIds.size(); from += MAX_CONCURRENT_SOURCING) {
            var batch = workflowIds.subList(from, Math.min(from + MAX_CONCURRENT_SOURCING, workflowIds.size()));
            result = result.thenCompose(previous -> {
                var loads = batch.stream().map(this::source).toList();
                return CompletableFuture.allOf(loads.toArray(CompletableFuture[]::new))
                                        .thenAccept(ignored -> loads.stream()
                                                                    .map(CompletableFuture::resultNow)
                                                                    .flatMap(Optional::stream)
                                                                    .forEach(sourced::add));
            });
        }
        return result.thenApply(ignored -> List.copyOf(sourced));
    }

    /**
     * Sources one workflow from the store in its own unit of work. Empty when the store holds no events for the id,
     * which is how an id a query names but nothing has recorded is told apart from an existing one.
     */
    private CompletableFuture<Optional<WorkflowState>> source(String workflowId) {
        return unitOfWorkFactory.create("workflow-manager-" + workflowId)
                                .executeWithResult(context -> workflowStore.findWorkflow(workflowId, context));
    }

    /**
     * Resolves the live workflows matching a query: the executions in memory, plus the hibernated workflows the query
     * may match and the workflows that wake while the query looks those up, that still match it and are not terminal.
     */
    private CompletableFuture<List<WorkflowState>> matchingLive(WorkflowStateQuery query) {
        var named = explicitWorkflowIds(query).distinct().toList();
        var hibernated = hibernatedLookup(query, named);
        var woken = ConcurrentHashMap.<String>newKeySet();
        return whileListeningForWakes(woken::add, () -> hibernated.get().thenCompose(
                before -> executionRepository.findAll(query).thenCompose(
                        executions -> hibernated.get().thenApply(after -> {
                            var byId = new LinkedHashMap<String, WorkflowState>();
                            executions.forEach(e -> byId.putIfAbsent(e.state().workflowId(), e.state()));
                            var candidates = new LinkedHashSet<>(before);
                            candidates.addAll(after);
                            candidates.removeAll(byId.keySet());
                            return new Merge(byId, candidates);
                        })
                )
        )).thenCompose(merge -> resolve(query, merge, woken, named, true));
    }

    /**
     * Returns a lookup of the hibernated workflows the given query may match: those it names, when it names workflow
     * ids; otherwise those the history finds for it, or every hibernated workflow when no history is projected. Each
     * lookup answers for the workflows hibernated at that moment. Only the last kind copies the identifiers of every
     * hibernated workflow: the others check just the ids the query names or the history finds, and query the history
     * at most once, and not at all while no workflow is hibernated.
     * <p>
     * A query looks up the hibernated workflows both before and after it lists the executions in memory, and listens
     * for the workflows that wake from before the first lookup until after the second, so it does not miss a workflow
     * that wakes or hibernates while it lists the workflows: a workflow that wakes enters memory before it leaves the
     * hibernated ones, one that hibernates joins them before it leaves memory, and one that hibernates after the first
     * lookup and wakes before the second reports its wake to the query.
     */
    private Supplier<CompletableFuture<Set<String>>> hibernatedLookup(WorkflowStateQuery query, List<String> named) {
        if (!named.isEmpty()) {
            return () -> CompletableFuture.completedFuture(hibernatedAmong(named));
        }
        if (!projectedHistory) {
            return () -> CompletableFuture.completedFuture(Set.copyOf(hibernatedWorkflowIds.get()));
        }
        var history = new AtomicReference<@Nullable CompletableFuture<List<String>>>();
        return () -> {
            if (hibernatedWorkflowIds.get().isEmpty()) {
                return CompletableFuture.completedFuture(Set.of());
            }
            var found = history.get();
            if (found == null) {
                found = historyRepository.findAll(query)
                                         .thenApply(matches -> matches.stream()
                                                                      .map(WorkflowHistory::workflowId)
                                                                      .distinct()
                                                                      .toList());
                history.set(found);
            }
            return found.thenApply(this::hibernatedAmong);
        };
    }

    /**
     * Returns those of the given workflows that are hibernated, in the given order.
     */
    private Set<String> hibernatedAmong(List<String> workflowIds) {
        var hibernated = hibernatedWorkflowIds.get();
        var result = new LinkedHashSet<String>();
        workflowIds.stream().filter(hibernated::contains).forEach(result::add);
        return result;
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

    private record Merge(LinkedHashMap<String, WorkflowState> byId, LinkedHashSet<String> candidates) {

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
                    ), false));
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
                    ), 0));
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
                    ), null));
                }
                return CompletableFuture.allOf(operations.toArray(CompletableFuture[]::new));
            });
        }
    }
}
