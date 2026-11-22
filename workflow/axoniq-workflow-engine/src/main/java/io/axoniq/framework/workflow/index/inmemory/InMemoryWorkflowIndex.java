/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.inmemory;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * A {@link WorkflowIndex} keeping the index of running workflow instances in memory.
 * <p>
 * The index starts out not initialized, so the first claim of every workflow module reads the running workflow
 * instances from the event store and initializes it; later claims within the same JVM load their running instances
 * from memory and only read the lifecycle events after the token they resume from.
 * <p>
 * That is only correct as long as this JVM is the only one processing the segments of the workflow modules using the
 * index: the engine relies on every change up to a stored token being in the index, and changes written to the index of
 * another node are not. Use it in tests, or in an application that runs a single node. Every other application uses a
 * durable index shared by all nodes, or {@link WorkflowIndex#none() no index} at all, which is the default. A restart is safe: the index starts out not
 * initialized again.
 * <p>
 * Operations complete immediately, and a load never observes a write half applied.
 * <p>
 * To configure it, register it with the {@link io.axoniq.framework.workflow.configuration.WorkflowConfigurer}:
 * <pre>{@code
 * WorkflowConfigurer.create()
 *                   .registerWorkflowIndex(config -> new InMemoryWorkflowIndex());
 * }</pre>
 * <p>
 * Marked {@link Internal} as a warning that API stability is not promised yet: this class is usable, and supported
 * for configuring the workflow index and for custom implementations, but its shape may still change in a minor
 * release.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
public class InMemoryWorkflowIndex implements WorkflowIndex {

    private final Map<String, Index> indices = new ConcurrentHashMap<>();

    /**
     * Creates an empty in-memory index, initialized for no workflow module.
     */
    public InMemoryWorkflowIndex() {
        // An index starts out not initialized: the first claim reads the running workflows from the event store.
    }

    @Override
    public CompletableFuture<Optional<Set<String>>> load(String indexName, Segment segment) {
        requireNonNull(segment, "The segment must not be null.");
        return CompletableFuture.completedFuture(index(indexName).load(segment));
    }

    @Override
    public CompletableFuture<Void> store(String indexName, IndexDelta delta) {
        requireNonNull(delta, "The delta must not be null.");
        index(indexName).store(delta);
        return FutureUtils.emptyCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> initialize(String indexName, Set<String> runningWorkflowIds) {
        requireNonNull(runningWorkflowIds, "The running workflow ids must not be null.");
        index(indexName).initialize(runningWorkflowIds);
        return FutureUtils.emptyCompletedFuture();
    }

    private Index index(String indexName) {
        return indices.computeIfAbsent(requireNonNull(indexName, "The index name must not be null."),
                                       name -> new Index());
    }

    /**
     * The entries of one index, and whether it was initialized. Synchronized, so a load never observes a write half
     * applied.
     */
    private static final class Index {

        private final Set<String> runningWorkflowIds = new HashSet<>();
        private boolean initialized;

        synchronized Optional<Set<String>> load(Segment segment) {
            if (!initialized) {
                return Optional.empty();
            }
            return Optional.of(runningWorkflowIds.stream()
                                                 .filter(segment::matches)
                                                 .collect(Collectors.toUnmodifiableSet()));
        }

        synchronized void store(IndexDelta delta) {
            runningWorkflowIds.removeAll(delta.removed());
            runningWorkflowIds.addAll(delta.added());
        }

        synchronized void initialize(Set<String> ids) {
            runningWorkflowIds.addAll(ids);
            initialized = true;
        }
    }
}
