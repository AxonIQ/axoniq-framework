/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.api;

import org.axonframework.common.FutureUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * The index returned by {@link WorkflowIndex#none()}: it keeps nothing and is never initialized.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
final class NoWorkflowIndex implements WorkflowIndex {

    static final NoWorkflowIndex INSTANCE = new NoWorkflowIndex();

    private NoWorkflowIndex() {
        // A single instance suffices: it keeps nothing.
    }

    @Override
    public CompletableFuture<Optional<Set<Entry>>> load(String indexName, Segment segment) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Void> store(String indexName, IndexDelta delta) {
        return FutureUtils.emptyCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> initialize(String indexName, Set<String> runningWorkflowIds) {
        return FutureUtils.emptyCompletedFuture();
    }

    @Override
    public String toString() {
        return "WorkflowIndex.none()";
    }
}
