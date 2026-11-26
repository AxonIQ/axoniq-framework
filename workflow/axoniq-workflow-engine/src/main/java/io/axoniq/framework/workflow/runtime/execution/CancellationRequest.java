/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A cancellation request for one workflow: what it makes against the {@link WorkflowCancellation} of the execution of
 * the workflow, and how it is made again, wherever the workflow is then.
 * <p>
 * The {@link WorkflowCancellationService} creates one for each request, and routes it through the
 * {@link CancellationRoute} of the execution in memory, which makes it against the execution or queues it as a
 * {@link QueuedRequest}. A request queued on an execution that leaves memory before its body took it is made again,
 * through the service.
 *
 * @param against makes the request against the cancellation of an execution
 * @param again   makes the request again, through the cancellation service, wherever the workflow is then
 * @param <T>     the type of the outcome of the request
 * @author Allard Buijze
 * @since 5.5.0
 */
record CancellationRequest<T>(Function<WorkflowCancellation, CompletableFuture<T>> against,
                              Supplier<CompletableFuture<T>> again) {

    /**
     * Creates a request.
     *
     * @param against makes the request against the cancellation of an execution
     * @param again   makes the request again, through the cancellation service, wherever the workflow is then
     */
    CancellationRequest {
        Objects.requireNonNull(against, "The request must not be null.");
        Objects.requireNonNull(again, "The request made again must not be null.");
    }

    /**
     * Makes this request against the given cancellation of an execution.
     *
     * @param cancellation the cancellation of the execution
     * @return the outcome of the request
     */
    CompletableFuture<T> makeAgainst(WorkflowCancellation cancellation) {
        return against.apply(cancellation);
    }

    /**
     * Makes this request again, through the cancellation service, wherever the workflow is then.
     *
     * @return the outcome of the request
     */
    CompletableFuture<T> makeAgain() {
        return again.get();
    }
}
