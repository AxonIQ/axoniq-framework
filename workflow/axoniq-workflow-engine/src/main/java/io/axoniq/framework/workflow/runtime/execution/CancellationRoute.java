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

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The route a cancellation request takes to one execution in memory: made against the cancellation of the execution
 * right away, queued as a task of the execution, or refused. Each {@link LiveWorkflows.InMemory entry in memory} has
 * its own route, so a workflow that leaves memory and enters it again, also by waking, gets a new one.
 * <p>
 * Two things decide the route of a request:
 * <ul>
 *     <li>Whether the execution woke from hibernation, which is fixed for its stay in memory. A woken body registers
 *     its running steps again only as it replays them, and takes a task only once it waits again where it hibernated,
 *     so every request for a woken execution is {@link #queue(QueuedRequest) queued} as its task, whose body makes the
 *     request once it takes the task, on the thread that also decides whether the execution hibernates.</li>
 *     <li>The {@link Phase phase} of the execution. A request is made against an execution that
 *     {@link Started started}, refused for a restored one that is {@link AwaitingStart awaiting its start}, as its
 *     claim did not catch up, and refused once it {@link Left left} memory. A new execution is marked started as it
 *     enters memory, also while its claim catches up, so it never refuses a request for awaiting its start.</li>
 * </ul>
 * Each transition is a compare-and-set on the phase, so no transition needs a lock of its own:
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
 *     last, so an entry in the entries is {@link Left} only while the engine handles the failed start of a woken
 *     execution under the lock of its workflow, which every request takes.</li>
 * </ul>
 * <b>Leaving takes no lock.</b> So a request that reached a started execution checks the phase once more after it was
 * made, and aborts a workflow cancellation it made pending once the execution left, see
 * {@link #request(Function, Supplier)}; the leave aborts the one pending as it leaves. A queued request checks the
 * phase once it is queued, see {@link #queue(QueuedRequest)}. Each queued request is taken by the body or answered by
 * the leave, once: whichever removes it from the queued requests first.
 * <p>
 * <b>Answering the queued requests.</b> When the execution leaves memory before its body took a queued request, the
 * leave answers it: made again, wherever the workflow is then, or, when the start of the woken execution failed, failed
 * with that failure. It is answered on the executor of this route, never on the thread on which the execution leaves
 * memory: that thread may hold the lock of a claim, and a request made again takes the lock of its workflow, which the
 * engine takes before the lock of a claim. Only when the executor no longer accepts tasks, on shutdown, is the request
 * answered on the calling thread.
 * <p>
 * The {@link WorkflowCancellationService} routes a request through the route of the workflow's entry, under the lock
 * of the workflow. The {@link LiveWorkflows} registry moves the route through its phases as the engine starts the body
 * of the execution and as the execution leaves memory.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
final class CancellationRoute {

    private final WorkflowExecution execution;
    // Non-null when the execution woke from hibernation: fixed for the stay in memory, unlike the phase.
    private final @Nullable Executor answerOn;
    private final AtomicReference<Phase> phase = new AtomicReference<>(new AwaitingStart());
    private final Set<QueuedRequest<?>> queuedRequests = ConcurrentHashMap.newKeySet();

    private CancellationRoute(WorkflowExecution execution, @Nullable Executor answerOn) {
        this.execution = Objects.requireNonNull(execution, "The WorkflowExecution must not be null.");
        this.answerOn = answerOn;
    }

    /**
     * Returns the route to an execution that starts, or that a claim restores: a request is made against its
     * cancellation once its body started.
     *
     * @param execution the execution that enters memory
     * @return the route of the requests to the execution, awaiting its start
     */
    static CancellationRoute toStarted(WorkflowExecution execution) {
        return new CancellationRoute(execution, null);
    }

    /**
     * Returns the route to an execution that woke from hibernation: every request is queued as its task.
     *
     * @param execution the execution that woke
     * @param answerOn  answers the requests queued on the execution when it leaves memory before its body took them
     * @return the route of the requests to the execution, awaiting its start
     */
    static CancellationRoute toWoken(WorkflowExecution execution, Executor answerOn) {
        return new CancellationRoute(execution, Objects.requireNonNull(answerOn, "The executor must not be null."));
    }

    /**
     * Returns the exception a request for a workflow that takes no requests on this node fails with.
     *
     * @param workflowId the identifier of the workflow
     * @return the exception to throw
     */
    static NoSuchElementException noRunningWorkflow(String workflowId) {
        return new NoSuchElementException("No running workflow found with id '" + workflowId + "'");
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
     * aborts a workflow cancellation the execution did not perform yet, unless it hibernated, and answers the queued
     * requests by making them again.
     *
     * @param departure how the execution left memory
     */
    void leave(Departure departure) {
        leave(new Left(departure, null));
    }

    /**
     * The <i>leave</i> transition of a woken execution whose start failed, and which leaves memory next. It takes no
     * request from now on: the requests queued on it fail with the given failure, and a workflow cancellation it did
     * not perform yet is aborted, also one its body was making as the start failed.
     *
     * @param failure the failure of the start
     */
    void failStart(Throwable failure) {
        leave(new Left(Departure.FAILED_TO_START, Objects.requireNonNull(failure, "The failure must not be null.")));
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
        answerQueued(left);
    }

    /**
     * Makes the given request, as far as this route takes it: queued as a task of an execution that woke from
     * hibernation, whether its body started or not, or made against the cancellation of an execution whose body
     * started. The caller holds the lock of the workflow.
     *
     * @param request makes the request against the cancellation of the execution
     * @param again   makes the request again, wherever the workflow is then, when it was queued on an execution that
     *                left memory before its body took it
     * @param <T>     the type of the outcome of the request
     * @return the outcome of the request
     * @throws NoSuchElementException when the execution takes no requests: it awaits its start, as its claim did not
     *                                catch up, or it left memory
     */
    <T> CompletableFuture<T> request(Function<WorkflowCancellation, CompletableFuture<T>> request,
                                     Supplier<CompletableFuture<T>> again) {
        return switch (phase.get()) {
            // It left memory after the caller found its entry, as leaving memory takes no lock; or it is a woken
            // execution whose start failed, whose entry stays until the engine, under the lock of the workflow, takes
            // it out of memory.
            case Left left -> throw noRunningWorkflow(execution.workflowId());
            case Phase woken when answerOn != null -> {
                var queued = new QueuedRequest<>(request, again);
                queue(queued);
                yield queued.result();
            }
            case Started(var cancellation) -> makeAgainst(cancellation, request);
            case AwaitingStart() -> throw noRunningWorkflow(execution.workflowId());
        };
    }

    /**
     * Queues the given request as a task of the execution, which woke from hibernation, and answers it right away when
     * the execution left memory meanwhile: a leave changes the phase to {@link Left} and then answers the queued
     * requests, while this adds the request and then checks whether the execution left. When the execution takes no
     * task, for example because its task queue is full, nothing stays queued: the request fails with the reason it
     * could not be queued.
     *
     * @param queued the request to queue
     * @throws IllegalStateException when the execution did not wake from hibernation, so it takes no queued requests
     */
    void queue(QueuedRequest<?> queued) {
        if (answerOn == null) {
            throw new IllegalStateException(
                    "Workflow '%s' did not wake from hibernation.".formatted(execution.workflowId())
            );
        }
        queuedRequests.add(queued);
        try {
            execution.appendTask(taking -> {
                if (queuedRequests.remove(queued)) {
                    queued.makeOn(this);
                }
            });
        } catch (RuntimeException e) {
            if (queuedRequests.remove(queued)) {
                answer(queued, e);
            }
            return;
        }
        if (phase.get() instanceof Left left) {
            answerQueued(left);
        }
    }

    /**
     * Returns whether a cancellation request reaches the execution, directly or queued as a task, see
     * {@link #request(Function, Supplier)}.
     *
     * @return {@code true} when a request reaches the execution
     */
    boolean takesRequests() {
        return switch (phase.get()) {
            case Left left -> false;
            case Started started -> true;
            // Queued as a task of an execution that woke from hibernation until its body starts.
            case AwaitingStart() -> answerOn != null;
        };
    }

    /**
     * Returns whether a cancellation request is queued as a task of the execution, as it woke from hibernation and did
     * not leave memory.
     *
     * @return {@code true} when a request is queued as a task of the execution
     */
    boolean queuesRequests() {
        return answerOn != null && !(phase.get() instanceof Left);
    }

    /**
     * Makes the given request against the given cancellation of the execution. A leave takes no lock: it changes the
     * phase to {@link Left} and then aborts a pending workflow cancellation, while this request makes one pending and
     * then checks whether the execution left, so one of the two aborts a request that the execution never performs.
     */
    private <T> CompletableFuture<T> makeAgainst(WorkflowCancellation cancellation,
                                                 Function<WorkflowCancellation, CompletableFuture<T>> request) {
        var result = request.apply(cancellation);
        if (phase.get() instanceof Left left) {
            abortPendingWorkflowCancellation(cancellation, left);
        }
        return result;
    }

    /**
     * Answers each queued request once, as the given departure says: fails it with the failure of a start that failed,
     * or makes it again.
     */
    private void answerQueued(Left left) {
        queuedRequests.forEach(queued -> {
            if (queuedRequests.remove(queued)) {
                answer(queued, left.failure());
            }
        });
    }

    /**
     * Answers the given request on the executor of this route, or on the calling thread once that executor no longer
     * accepts tasks: fails it with the given failure, or makes it again without one.
     */
    private void answer(QueuedRequest<?> queued, @Nullable Throwable failure) {
        Runnable answer = () -> {
            if (failure == null) {
                queued.makeAgain();
            } else {
                queued.fail(failure);
            }
        };
        try {
            Objects.requireNonNull(answerOn, "Only a woken execution has queued requests.").execute(answer);
        } catch (RejectedExecutionException e) {
            answer.run();
        }
    }

    /**
     * Aborts a workflow cancellation pending on the given cancellation, as the execution left memory the given way,
     * unless it hibernated, which it only does without one.
     */
    private static void abortPendingWorkflowCancellation(WorkflowCancellation cancellation, Left left) {
        var abortion = left.abortion();
        if (abortion != null && cancellation instanceof WorkflowCancellation.Request request) {
            request.abortPendingWorkflowCancellation(abortion);
        }
    }

    /**
     * How an execution left memory, and why a workflow cancellation it did not perform yet can no longer be performed.
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
         * Its start failed: a woken execution, which then swaps back for its hibernation or leaves memory. The
         * {@link Left} phase carries the failure, see {@link #failStart(Throwable)}.
         */
        FAILED_TO_START("Workflow execution failed to start before cancellation was performed"),
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
     * An execution whose body started: a request is made against its cancellation right away, unless the execution
     * woke from hibernation, which still gets its requests queued as its tasks.
     *
     * @param cancellation the cancellation of the execution
     */
    private record Started(WorkflowCancellation cancellation) implements Phase {

    }

    /**
     * An execution that left memory, or whose start failed so it leaves next: it takes no requests.
     *
     * @param departure how it left, which tells whether a pending workflow cancellation is aborted
     * @param failure   the failure of a start that failed, which fails the requests queued on it, or {@code null}
     *                  when they are made again, wherever the workflow is then
     */
    private record Left(Departure departure, @Nullable Throwable failure) implements Phase {

        private Left {
            if ((departure == Departure.FAILED_TO_START) != (failure != null)) {
                throw new IllegalArgumentException("Only an execution that failed to start leaves with a failure.");
            }
        }

        /**
         * Returns the reason a workflow cancellation the execution did not perform yet is aborted with, carrying the
         * failure of a start that failed, or {@code null} when the execution hibernated, which aborts none.
         */
        private @Nullable CancellationException abortion() {
            if (departure.abortReason == null) {
                return null;
            }
            var abortion = new CancellationException(departure.abortReason);
            if (failure != null) {
                abortion.initCause(failure);
            }
            return abortion;
        }
    }

    /**
     * A cancellation request queued as a task of an execution that woke from hibernation, see
     * {@link #queue(QueuedRequest)}: made by the body of the execution once it takes the task, or answered by the route
     * when the execution leaves memory before.
     *
     * @param <T> the type of the outcome of the request
     */
    static final class QueuedRequest<T> {

        private final Function<WorkflowCancellation, CompletableFuture<T>> request;
        private final Supplier<CompletableFuture<T>> again;
        private final CompletableFuture<T> result = new CompletableFuture<>();

        /**
         * Creates a request to queue.
         *
         * @param request makes the request against the cancellation of an execution
         * @param again   makes the request again, through the cancellation service, wherever the workflow is then
         */
        QueuedRequest(Function<WorkflowCancellation, CompletableFuture<T>> request,
                      Supplier<CompletableFuture<T>> again) {
            this.request = Objects.requireNonNull(request, "The request must not be null.");
            this.again = Objects.requireNonNull(again, "The request made again must not be null.");
        }

        /**
         * Returns the outcome of the request.
         *
         * @return a future completing with the outcome of the request, wherever it is made
         */
        CompletableFuture<T> result() {
            return result;
        }

        /**
         * Makes the request again, through the cancellation service, on the calling thread.
         */
        void makeAgain() {
            completeWith(again);
        }

        /**
         * Fails the request with the given failure.
         *
         * @param failure the failure of the request
         */
        void fail(Throwable failure) {
            result.completeExceptionally(failure);
        }

        /**
         * Makes the request against the cancellation of the execution of the given route, whose body took it. A body
         * only runs once its execution started, so this only answers the request instead when the execution left
         * memory meanwhile, which takes no lock, as that departure says.
         */
        private void makeOn(CancellationRoute route) {
            switch (route.phase.get()) {
                case Started(var cancellation) -> completeWith(() -> route.makeAgainst(cancellation, request));
                case Left left -> route.answer(this, left.failure());
                // Not seen: the engine marks an execution started before its body runs.
                case AwaitingStart() -> route.answer(this, null);
            }
        }

        private void completeWith(Supplier<CompletableFuture<T>> outcome) {
            try {
                outcome.get().whenComplete((value, failure) -> {
                    if (failure == null) {
                        result.complete(value);
                    } else {
                        result.completeExceptionally(failure);
                    }
                });
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
        }
    }
}
