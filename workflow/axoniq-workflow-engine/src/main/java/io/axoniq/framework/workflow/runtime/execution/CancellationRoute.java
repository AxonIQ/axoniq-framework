/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The route a cancellation request takes to one execution in memory. Each {@link LiveWorkflows.InMemory entry in
 * memory} has its own route, so a workflow that leaves memory and enters it again, also by waking, gets a new one.
 * <p>
 * <b>Delivery.</b> How a request reaches the execution is fixed for the stay in memory, as the route is created:
 * <ul>
 *     <li>{@link #direct()}, to an execution that started, or that a claim restored: a request is made against the
 *     cancellation of the execution right away, once its body started;</li>
 *     <li>{@link #queued(WorkflowExecution, Executor)}, to an execution that woke from hibernation: a woken body
 *     registers its running steps again only as it replays them, and takes a task only once it waits again where it
 *     hibernated, so every request is queued as a task of the execution, whose body makes it once it takes the
 *     task.</li>
 * </ul>
 * <b>Phase.</b> Whether a request reaches the execution at all depends on its {@link Phase phase}. A restored
 * execution {@link AwaitingStart awaits its start} until its claim caught up; a new one is marked {@link Started
 * started} as it enters memory, also while its claim catches up. Each transition is a compare-and-set on the phase, so
 * no transition needs a lock of its own:
 * <table>
 *     <caption>Phase transitions</caption>
 *     <tr><th>Phase</th><th><i>start</i></th><th><i>leave</i></th></tr>
 *     <tr><td>{@link AwaitingStart}</td><td>{@link Started}</td><td>{@link Left}</td></tr>
 *     <tr><td>{@link Started}</td><td>unchanged</td><td>{@link Left}</td></tr>
 *     <tr><td>{@link Left}</td><td>unchanged</td><td>unchanged: the first departure counts</td></tr>
 * </table>
 * <ul>
 *     <li><i>start</i>, see {@link #start(WorkflowCancellation)}: the engine starts the body of the execution, under
 *     the lock of the claim of the execution, if any. A new execution is marked started as it enters memory, under the
 *     lock of its claim, whether its body starts then or once the claim caught up. A restored execution starts with
 *     the restored executions of its claim, as the claim caught up. A woken execution whose claim caught up starts as
 *     it wakes, so under the lock of its workflow as well; one that woke while its claim caught up starts with the
 *     restored executions of that claim, under the lock of the claim only.</li>
 *     <li><i>leave</i>, see {@link #leave(Departure)}: the execution left memory, as it ended or stopped (no lock),
 *     hibernated or swapped back for its hibernation (the lock of the workflow), or the engine shut down (no lock); or,
 *     see {@link #failStart(Throwable)}, the start of a woken execution failed, which leaves memory next (the lock of
 *     the workflow). The {@link LiveWorkflows} registry removes the entry from its entries before every leave but the
 *     last, so an entry in the entries is {@link Left} only while the engine's hibernation handles the failed start of
 *     a woken execution under the lock of its workflow, which every request takes.</li>
 * </ul>
 * <b>A request</b>, see {@link #request(CancellationRequest)}, goes where its delivery and the phase say:
 * <table>
 *     <caption>Requests</caption>
 *     <tr><th>Phase</th><th>direct</th><th>queued</th></tr>
 *     <tr><td>{@link AwaitingStart}</td><td>refused</td><td>queued</td></tr>
 *     <tr><td>{@link Started}</td><td>made now</td><td>queued</td></tr>
 *     <tr><td>{@link Left}</td><td>refused</td><td>refused</td></tr>
 * </table>
 * <b>Leaving takes no lock</b>, see the {@link io.axoniq.framework.workflow.runtime.execution package}. So a request
 * made now checks the phase once more after it was made, and aborts a workflow cancellation it made pending once the
 * execution left, see {@link #makeNow(WorkflowCancellation, CancellationRequest)}; the leave aborts the one pending as
 * it leaves. A queued request checks the phase once it is queued, see {@link Queued#add(QueuedRequest)}.
 * <p>
 * The {@link WorkflowCancellationService} routes a request through the route of the workflow's entry, under the lock
 * of the workflow. The {@link LiveWorkflows} registry creates the route of each entry, and moves it through its phases
 * as the engine starts the body of the execution and as the execution leaves memory.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
final class CancellationRoute {

    private final AtomicReference<Phase> phase = new AtomicReference<>(new AwaitingStart());
    private final Delivery delivery;

    private CancellationRoute() {
        this.delivery = new Direct();
    }

    private CancellationRoute(WorkflowExecution execution, Executor answerOn) {
        this.delivery = new Queued(execution, answerOn);
    }

    /**
     * Creates the route to an execution that starts, or that a claim restores, awaiting its start: a request is made
     * against the cancellation of the execution once its body started, and refused before and once it left memory.
     *
     * @return a new route that makes requests directly
     */
    static CancellationRoute direct() {
        return new CancellationRoute();
    }

    /**
     * Creates the route to an execution that woke from hibernation, awaiting its start: every request is queued as a
     * task of the execution, whether its body started or not, and refused once it left memory.
     *
     * @param execution the execution that woke
     * @param answerOn  answers the requests queued on the execution when it leaves memory before its body took them
     * @return a new route that queues requests
     */
    static CancellationRoute queued(WorkflowExecution execution, Executor answerOn) {
        return new CancellationRoute(execution, answerOn);
    }

    /**
     * The <i>start</i> transition: the body of the execution starts, so requests reach it through the given
     * cancellation from now on. Changes nothing once the execution started or left.
     *
     * @param cancellation the cancellation of the execution
     */
    void start(WorkflowCancellation cancellation) {
        Objects.requireNonNull(cancellation, "The WorkflowCancellation must not be null.");
        phase.updateAndGet(current -> current instanceof AwaitingStart ? new Started(cancellation) : current);
    }

    /**
     * The <i>leave</i> transition: the execution left memory the given way. Only the first departure counts: the winner
     * aborts a workflow cancellation the execution did not perform yet, if any, unless it hibernated, and makes the
     * queued requests again, wherever the workflow is then.
     *
     * @param departure how the execution left memory
     */
    void leave(Departure departure) {
        leave(new Departed(Objects.requireNonNull(departure, "The Departure must not be null.")));
    }

    /**
     * The <i>leave</i> transition of an execution whose start failed, and which leaves memory next: only a woken
     * execution, whose start the engine's hibernation runs. It takes no request from now on: the requests queued on it
     * fail with the given failure, and a workflow cancellation it did not perform yet, if any, is aborted, also one its
     * body was making as the start failed.
     *
     * @param failure the failure of the start
     */
    void failStart(Throwable failure) {
        leave(new StartFailed(Objects.requireNonNull(failure, "The failure must not be null.")));
    }

    private void leave(Left left) {
        switch (phase.getAndUpdate(current -> current instanceof Left ? current : left)) {
            case Left earlier -> {
                // Only the first departure counts, and it answered the queued requests already.
                return;
            }
            case Started(var cancellation) -> abortPendingWorkflowCancellation(cancellation, left);
            case AwaitingStart() -> {
                // Its body never started, so nothing can be pending.
            }
        }
        if (delivery instanceof Queued queued) {
            queued.answerWaiting(left);
        }
    }

    /**
     * Makes the given request, as far as this route takes it, see the class documentation: made now against a started
     * execution of a direct route, queued on an execution of a queued route, or refused. The caller holds the lock of
     * the workflow.
     *
     * @param request the request to make
     * @param <T>     the type of the outcome of the request
     * @return the outcome of the request, or empty when the execution takes no requests: it awaits its start, as its
     * claim did not catch up, or it left memory
     */
    <T> Optional<CompletableFuture<T>> request(CancellationRequest<T> request) {
        var current = phase.get();
        return switch (delivery) {
            case Direct() -> current instanceof Started(var cancellation)
                    ? Optional.of(makeNow(cancellation, request))
                    : Optional.empty();
            case Queued queued -> current instanceof Left
                    ? Optional.empty()
                    : Optional.of(queued.add(request));
        };
    }

    /**
     * Returns whether a cancellation request reaches the execution now, see {@link #request(CancellationRequest)}.
     *
     * @return {@code true} when a request reaches the execution
     */
    boolean takesRequests() {
        var current = phase.get();
        return switch (delivery) {
            case Direct() -> current instanceof Started;
            case Queued queued -> !(current instanceof Left);
        };
    }

    /**
     * Queues the given request as a task of the execution, when this route queues requests: a wake of the workflow
     * hands its woken execution the request before its body starts. Unlike {@link #request(CancellationRequest)}, it
     * queues the request in every phase: when the execution left memory meanwhile, its departure answers the request
     * right away, see {@link Queued#add(QueuedRequest)}.
     *
     * @param request the request to queue
     * @return {@code true} when the request was queued, {@code false} when this route makes requests directly
     */
    boolean queue(QueuedRequest<?> request) {
        if (delivery instanceof Queued queued) {
            queued.add(request);
            return true;
        }
        return false;
    }

    /**
     * Makes the given request now, against the given cancellation of the started execution. A leave takes no lock: it
     * changes the phase to {@link Left} and then aborts a pending workflow cancellation, while this request makes one
     * pending and then checks whether the execution left, so one of the two aborts a request that the execution never
     * performs.
     *
     * @param cancellation the cancellation of the started execution
     * @param request      the request to make against that cancellation
     * @param <T>          the type of the outcome of the request
     * @return the outcome of the request
     */
    private <T> CompletableFuture<T> makeNow(WorkflowCancellation cancellation, CancellationRequest<T> request) {
        var result = request.makeAgainst(cancellation);
        if (phase.get() instanceof Left left) {
            abortPendingWorkflowCancellation(cancellation, left);
        }
        return result;
    }

    /**
     * Aborts a workflow cancellation pending on the given cancellation, if any, as the execution left memory the given
     * way, unless it hibernated, which it only does without one.
     */
    private static void abortPendingWorkflowCancellation(WorkflowCancellation cancellation, Left left) {
        var abortion = left.abortion();
        if (abortion != null && cancellation instanceof WorkflowCancellation.Request request) {
            request.abortPendingWorkflowCancellation(abortion);
        }
    }

    /**
     * How a request reaches the execution: {@link Direct} or {@link Queued}, see the class documentation.
     */
    private sealed interface Delivery {

    }

    /**
     * Requests are made against the cancellation of the execution once its body started, and refused before. Nothing
     * is queued, so a leave has nothing to answer.
     */
    private record Direct() implements Delivery {

    }

    /**
     * Every request waits as a task of the execution, whether its body started or not, until the body takes it. It
     * belongs to its route, whose phase it reads as it queues a request and as the body takes one.
     * <p>
     * <b>Each queued request is answered exactly once</b>: taken by the body, or answered by the leave, whichever
     * removes it from the waiting requests first. A taken request is made now against the cancellation of the
     * execution, unless the execution left meanwhile.
     * <p>
     * <b>Answers run off the leaving thread.</b> When the execution leaves memory before its body took a queued
     * request, the leave answers it: made again, wherever the workflow is then, or, when the start of the woken
     * execution failed, failed with that failure. It is answered on the executor of this route, never on the thread on
     * which the execution leaves memory: that thread may hold the lock of a claim, and a request made again takes the
     * lock of its workflow, which comes first in the lock order of the
     * {@link io.axoniq.framework.workflow.runtime.execution package}. Only when the executor no longer accepts tasks,
     * on shutdown, is the request answered on the calling thread.
     */
    private final class Queued implements Delivery {

        private final WorkflowExecution execution;
        private final Executor answerOn;
        private final Set<QueuedRequest<?>> waiting = ConcurrentHashMap.newKeySet();

        private Queued(WorkflowExecution execution, Executor answerOn) {
            this.execution = Objects.requireNonNull(execution, "The WorkflowExecution must not be null.");
            this.answerOn = Objects.requireNonNull(answerOn, "The executor must not be null.");
        }

        /**
         * Queues a new request, see {@link #add(QueuedRequest)}.
         */
        private <T> CompletableFuture<T> add(CancellationRequest<T> request) {
            var queued = new QueuedRequest<>(request);
            add(queued);
            return queued.result();
        }

        /**
         * Queues the given request as a task of the execution, and answers it right away when the execution left
         * memory meanwhile: a leave changes the phase to {@link Left} and then answers the waiting requests, while this
         * adds the request and then checks whether the execution left. When the execution takes no task, for example
         * because its task queue is full, nothing stays queued: the request fails with the reason it could not be
         * queued.
         *
         * @param request the request to queue
         */
        private void add(QueuedRequest<?> request) {
            waiting.add(request);
            try {
                execution.appendTask(body -> {
                    if (waiting.remove(request)) {
                        makeTaken(request);
                    }
                });
            } catch (RuntimeException e) {
                if (waiting.remove(request)) {
                    answerOnExecutor(() -> request.fail(e));
                }
                return;
            }
            if (phase.get() instanceof Left left) {
                answerWaiting(left);
            }
        }

        /**
         * Makes the given request, which the body of the execution took as its task, now. A body only runs once its
         * execution started, so this only answers the request instead when the execution left memory meanwhile, which
         * takes no lock: it is made again, or failed with the failure of a start that failed, as the leave answers it.
         */
        private <T> void makeTaken(QueuedRequest<T> taken) {
            switch (phase.get()) {
                case Started(var cancellation) -> taken.completeWith(() -> makeNow(cancellation, taken.request()));
                case Left left -> answerOnExecutor(() -> left.answer(taken));
                // Not seen: the engine marks an execution started before its body runs.
                case AwaitingStart() -> answerOnExecutor(taken::makeAgain);
            }
        }

        /**
         * Answers the requests still waiting, as the execution left memory the given way before its body took them:
         * fails each with the failure of a start that failed, or makes it again. Runs for the first departure, after it
         * aborted a workflow cancellation pending on the execution, if any, and again for a request queued after that.
         */
        private void answerWaiting(Left left) {
            waiting.forEach(request -> {
                if (waiting.remove(request)) {
                    answerOnExecutor(() -> left.answer(request));
                }
            });
        }

        /**
         * Runs the given answer on the executor of this route, or on the calling thread once that executor no longer
         * accepts tasks.
         */
        private void answerOnExecutor(Runnable answer) {
            try {
                answerOn.execute(answer);
            } catch (RejectedExecutionException e) {
                answer.run();
            }
        }
    }

    /**
     * How an execution left memory, and why a workflow cancellation it did not perform yet can no longer be performed.
     * A start that failed is no departure: see {@link #failStart(Throwable)}.
     */
    enum Departure {

        /**
         * It hibernated, which it only does without a pending workflow cancellation, or it woke and swapped back for
         * its hibernation.
         */
        HIBERNATED(null),
        /**
         * It ended or stopped.
         */
        COMPLETED("Workflow execution completed before cancellation was performed"),
        /**
         * The engine shut down.
         */
        SHUT_DOWN("Workflow engine shut down before cancellation was performed");

        private final @Nullable String abortReason;

        Departure(@Nullable String abortReason) {
            this.abortReason = abortReason;
        }
    }

    /**
     * The phase of the execution: whether its body started, and whether it left memory, see the class documentation.
     */
    private sealed interface Phase {

    }

    /**
     * An execution whose body did not start: a restored one takes no requests yet, and its body starts once the claim
     * of its segment caught up; one that woke from hibernation gets its requests queued as its tasks. A new execution
     * leaves this phase as it enters memory, see the class documentation.
     */
    private record AwaitingStart() implements Phase {

    }

    /**
     * An execution whose body started: a request reaches it through its cancellation, made now or as a task.
     *
     * @param cancellation the cancellation of the execution
     */
    private record Started(WorkflowCancellation cancellation) implements Phase {

    }

    /**
     * An execution that left memory, or whose start failed so it leaves next: it takes no requests.
     */
    private sealed interface Left extends Phase {

        /**
         * Returns the reason a workflow cancellation the execution did not perform yet is aborted with.
         *
         * @return the reason of the abortion, or {@code null} when the execution hibernated, which aborts none
         */
        @Nullable CancellationException abortion();

        /**
         * Answers the given request, which was queued on the execution and not taken by its body.
         *
         * @param request the request to answer
         */
        void answer(QueuedRequest<?> request);
    }

    /**
     * An execution that left memory the given way. The requests queued on it are made again, wherever the workflow is
     * then.
     *
     * @param departure how it left, which tells whether a pending workflow cancellation is aborted
     */
    private record Departed(Departure departure) implements Left {

        @Override
        public @Nullable CancellationException abortion() {
            return departure.abortReason == null ? null : new CancellationException(departure.abortReason);
        }

        @Override
        public void answer(QueuedRequest<?> request) {
            request.makeAgain();
        }
    }

    /**
     * A woken execution whose start failed, which leaves memory next. The requests queued on it fail with the failure.
     *
     * @param failure the failure of the start
     */
    private record StartFailed(Throwable failure) implements Left {

        @Override
        public CancellationException abortion() {
            var abortion = new CancellationException("Workflow execution failed to start before cancellation was "
                                                             + "performed");
            abortion.initCause(failure);
            return abortion;
        }

        @Override
        public void answer(QueuedRequest<?> request) {
            request.fail(failure);
        }
    }
}
