/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A cancellation request queued as a task of an execution that woke from hibernation: a promise of its outcome, which
 * the {@link CancellationRoute route} it is queued on completes, by making the request once the body took it, by
 * making it again, or by failing it.
 * <p>
 * The {@link WorkflowCancellationService} creates one for a hibernated workflow before it has the workflow woken, and
 * answers it itself when the wake did not queue it, see {@link #answerUnqueued(String, Throwable, boolean)}.
 *
 * @param <T> the type of the outcome of the request
 * @author Allard Buijze
 * @since 5.5.0
 */
final class QueuedRequest<T> {

    private final CancellationRequest<T> request;
    private final CompletableFuture<T> result = new CompletableFuture<>();

    /**
     * Creates a request to queue.
     *
     * @param request the request to make once the body of the execution took it
     */
    QueuedRequest(CancellationRequest<T> request) {
        this.request = Objects.requireNonNull(request, "The request must not be null.");
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
     * Returns the request to make once the body of the execution took it.
     *
     * @return the request
     */
    CancellationRequest<T> request() {
        return request;
    }

    /**
     * Completes the request with the given outcome, or with the failure the given outcome throws.
     *
     * @param outcome returns the outcome of the request
     */
    void completeWith(Supplier<CompletableFuture<T>> outcome) {
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

    /**
     * Makes the request again, through the cancellation service, on the calling thread.
     */
    void makeAgain() {
        completeWith(request::makeAgain);
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
     * Answers this request for the given hibernated workflow, which the wake for it did not queue on the execution it
     * woke, once that wake ran:
     * <ul>
     *     <li>fails it with the failure of the wake, if any;</li>
     *     <li>fails it when the workflow is hibernated once the wake ran: mostly as its wake failed before the workflow
     *     was in memory, which is tried again later, while the request is not; or, in a tiny window, because another
     *     wake woke the workflow first and it hibernated again before this answer;</li>
     *     <li>else makes it again, wherever the workflow is now: queued on its execution in memory when another wake,
     *     such as one for an event, restored it first, whose steps may not be registered again yet; made against the
     *     execution a claim restored meanwhile, or refused while that claim did not catch up, as such an execution
     *     takes no request before its body starts; or refused when the workflow left this node, also when its own
     *     wake restored it and it left memory again before the request was queued.</li>
     * </ul>
     *
     * @param workflowId      the identifier of the workflow the request is for
     * @param wakeFailure     the failure of the wake, or {@code null} when it ran without one
     * @param stillHibernated whether the workflow is hibernated once the wake ran
     */
    void answerUnqueued(String workflowId, @Nullable Throwable wakeFailure, boolean stillHibernated) {
        if (wakeFailure != null) {
            fail(wakeFailure);
        } else if (stillHibernated) {
            fail(new IllegalStateException("Workflow '" + workflowId + "' could not be woken from hibernation"));
        } else {
            makeAgain();
        }
    }
}
