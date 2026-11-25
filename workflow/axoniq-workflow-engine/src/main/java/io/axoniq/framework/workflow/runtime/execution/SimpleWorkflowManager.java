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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/**
 * Workflow Manager backed by the workflow history projection and the {@link LiveWorkflows live workflows} of its
 * workflow module.
 * <p>
 * A workflow whose execution is in memory answers a query for itself. Every other workflow a query may match is sourced
 * from the {@link WorkflowStore}, so the answer for a workflow that is not in memory is the event store's, never a
 * projection that may still lag behind it. That includes the hibernated workflows, which are live, but have no
 * execution in memory. A query sources only the hibernated workflows it may match: those it names, when it names
 * workflow ids; otherwise those the history finds for it, or, when no history is projected, every hibernated workflow.
 * <p>
 * <b>Finding the live workflows.</b> {@link LiveWorkflows} holds one entry per live workflow, in memory or hibernated,
 * in one map, which a query reads without a lock:
 * <ul>
 *     <li>A query that names workflow ids reads the entry of each of them.</li>
 *     <li>A query that names none, on a module that projects no history, visits every entry once.</li>
 *     <li>A query that names none, on a module that projects its history, lists the executions in memory that match
 *     it, reads the entry of each workflow the history finds for it, and, when the engine may hibernate workflows,
 *     lists the executions in memory once more.</li>
 * </ul>
 * An entry in memory answers with the state of its execution, and a hibernated one is sourced. Without hibernation,
 * every live workflow is in memory, so a query for the live workflows that names no id, on a module that projects its
 * history, only lists them.
 * <p>
 * The key of a live workflow never leaves the map: entering memory adds it, leaving memory or this node removes it,
 * and every transition in between, hibernating, waking, or a restore that takes over a hibernation, replaces the entry
 * in place. So reading the entry of a workflow that stays live finds it, in memory or hibernated, whenever the read
 * runs, and visiting every entry visits it exactly once, as an iteration of a {@code ConcurrentHashMap} visits each key
 * that stays in it exactly once. A query that names workflow ids, or runs on a module that projects no history,
 * therefore never misses a workflow that hibernates, wakes, or hibernates and wakes while the query looks it up, and
 * never finds it twice: each workflow is found by its identifier, and its state is kept once, so a workflow found in
 * memory is not sourced as well.
 * <p>
 * A listing of the executions in memory misses a workflow that is hibernated as the listing passes it. A query that
 * names no id, on a module that projects its history, finds that workflow by reading its entry when the history holds
 * it, and by the second listing when it woke before that listing passed it. It misses a workflow only when the history
 * does not hold it, as its projection lags behind, and the workflow is hibernated both as the first listing passes it
 * and as the second one does. Finding such a workflow would take sourcing every hibernated workflow.
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
    private final LiveWorkflows liveWorkflows;
    private final WorkflowCancellationService cancellationService;
    private final WorkflowStore workflowStore;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Executor executor;
    private final BooleanSupplier hibernationEnabled;
    private final boolean projectedHistory;

    /**
     * Creates a Workflow Manager using a history projection and the live workflows of a workflow module, which may
     * hibernate.
     *
     * @param historyRepository   repository containing projected workflow states
     * @param liveWorkflows       the workflows of the module that are live on this node, in memory or hibernated
     * @param cancellationService service delivering cancellation requests to live workflows
     * @param workflowStore       store the state of a workflow that is not in memory is sourced from
     * @param unitOfWorkFactory   factory for the unit of work each such sourcing runs in
     * @param executor            executor used for asynchronous manager operations
     * @param hibernationEnabled  tells whether the engine of the module may hibernate workflows; without hibernation,
     *                            every live workflow is in memory
     * @param projectedHistory    whether the {@code historyRepository} is projected from the event store, so it finds
     *                            the hibernated workflows a query matches, {@code false} when the module projects no
     *                            history
     */
    public SimpleWorkflowManager(WorkflowHistoryRepository historyRepository,
                                 LiveWorkflows liveWorkflows,
                                 WorkflowCancellationService cancellationService,
                                 WorkflowStore workflowStore,
                                 UnitOfWorkFactory unitOfWorkFactory,
                                 Executor executor,
                                 BooleanSupplier hibernationEnabled,
                                 boolean projectedHistory) {
        this.hibernationEnabled = Objects.requireNonNull(hibernationEnabled,
                                                         "The hibernation-enabled supplier must not be null.");
        this.projectedHistory = projectedHistory;
        this.historyRepository = Objects.requireNonNull(historyRepository,
                                                        "The WorkflowHistoryRepository must not be null.");
        this.liveWorkflows = Objects.requireNonNull(liveWorkflows, "The LiveWorkflows must not be null.");
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
     * Resolves the states matching a query: those of the live workflows it matches, see the class documentation, and
     * those of the workflows the history finds for it and of the workflows it names that are no longer in memory, which
     * are sourced, so a workflow the projection has not seen yet is still found by id.
     */
    private CompletableFuture<List<WorkflowState>> matching(WorkflowStateQuery query) {
        return resolve(query, false);
    }

    /**
     * Resolves the live workflows matching a query, see the class documentation: the executions in memory, and the
     * hibernated workflows the query may match that, sourced, still match it and are not terminal.
     */
    private CompletableFuture<List<WorkflowState>> matchingLive(WorkflowStateQuery query) {
        return resolve(query, true);
    }

    private CompletableFuture<List<WorkflowState>> resolve(WorkflowStateQuery query, boolean liveOnly) {
        // Started inside a future, so a lookup that throws fails the query rather than escaping it.
        return CompletableFuture.completedFuture(new Found(query, liveOnly))
                                .thenCompose(this::lookUp)
                                .thenCompose(this::sourced);
    }

    /**
     * Finds the workflows the query of the given lookup may match, see the class documentation.
     */
    private CompletableFuture<Found> lookUp(Found found) {
        var query = found.query;
        var named = explicitWorkflowIds(query).distinct().toList();
        if (!named.isEmpty() || !projectedHistory) {
            if (!named.isEmpty()) {
                named.forEach(found::read);
            } else {
                liveWorkflows.entries().forEach(found::visit);
            }
            // A query for states also gets those of the workflows that are no longer live.
            return found.liveOnly ? CompletableFuture.completedFuture(found) : withHistory(found);
        }
        var listed = liveWorkflows.inMemory(query).thenApply(found::listed);
        if (!hibernationEnabled.getAsBoolean()) {
            // Every live workflow is in memory, so the listing finds them all.
            return found.liveOnly ? listed : listed.thenCompose(this::withHistory);
        }
        // A workflow that was hibernated as the first listing passed it, and that the history does not hold yet, is
        // found by the second listing once it woke.
        return listed.thenCompose(this::withHistory)
                     .thenCompose(lookup -> liveWorkflows.inMemory(query).thenApply(lookup::listed));
    }

    /**
     * Reads the entries of the workflows the history finds for the query of the given lookup.
     */
    private CompletableFuture<Found> withHistory(Found found) {
        return historyRepository.findAll(found.query).thenApply(matches -> {
            matches.forEach(match -> found.read(match.workflowId()));
            return found;
        });
    }

    /**
     * Sources the workflows the given lookup did not find in memory, and completes it into the states matching its
     * query.
     */
    private CompletableFuture<List<WorkflowState>> sourced(Found found) {
        found.toSource.removeAll(found.inMemory.keySet());
        return sourceAll(List.copyOf(found.toSource)).thenApply(sourced -> {
            for (WorkflowState state : sourced) {
                if (found.keeps(state)) {
                    found.inMemory.putIfAbsent(state.workflowId(), state);
                }
            }
            return List.copyOf(found.inMemory.values());
        });
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

    /**
     * What a query found: the states of the workflows in memory it matches, which answer for themselves, and the
     * workflows it sources from the {@link WorkflowStore}. Used by one query, on one thread at a time.
     */
    private final class Found {

        private final WorkflowStateQuery query;
        private final boolean liveOnly;
        private final LinkedHashMap<String, WorkflowState> inMemory = new LinkedHashMap<>();
        private final LinkedHashSet<String> toSource = new LinkedHashSet<>();

        private Found(WorkflowStateQuery query, boolean liveOnly) {
            this.query = query;
            this.liveOnly = liveOnly;
        }

        /**
         * Adds the executions in memory that a listing found for the query.
         */
        private Found listed(Set<WorkflowExecution> executions) {
            executions.forEach(execution -> {
                var state = execution.state();
                inMemory.putIfAbsent(state.workflowId(), state);
            });
            return this;
        }

        /**
         * Reads the entry of the given workflow. One in memory answers with its state when that matches the query, and
         * a hibernated one is sourced. Unless only live workflows are resolved, so is one in memory whose state does
         * not match, which the store answers for, and one that is not live.
         */
        private void read(String workflowId) {
            if (inMemory.containsKey(workflowId)) {
                return;
            }
            switch (liveWorkflows.get(workflowId)) {
                case LiveWorkflows.InMemory entry -> {
                    var state = entry.execution().state();
                    if (WorkflowStateQueryMatcher.matches(query, state)) {
                        inMemory.putIfAbsent(workflowId, state);
                    } else if (!liveOnly) {
                        toSource.add(workflowId);
                    }
                }
                case LiveWorkflows.Hibernated ignored -> toSource.add(workflowId);
                case null -> {
                    if (!liveOnly) {
                        toSource.add(workflowId);
                    }
                }
            }
        }

        /**
         * Visits an entry of every live workflow: one in memory answers with its state when that matches the query, and
         * a hibernated one is sourced.
         */
        private void visit(LiveWorkflows.LiveWorkflow entry) {
            switch (entry) {
                case LiveWorkflows.InMemory inMemoryEntry -> {
                    var state = inMemoryEntry.execution().state();
                    if (WorkflowStateQueryMatcher.matches(query, state)) {
                        inMemory.putIfAbsent(state.workflowId(), state);
                    }
                }
                case LiveWorkflows.Hibernated hibernated -> toSource.add(hibernated.workflowId());
            }
        }

        /**
         * Returns whether the given sourced state is kept: it matches the query and, when only live workflows are
         * resolved, is not terminal.
         */
        private boolean keeps(WorkflowState state) {
            return WorkflowStateQueryMatcher.matches(query, state)
                    && !(liveOnly && state.workflowStatus().isTerminal());
        }
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
