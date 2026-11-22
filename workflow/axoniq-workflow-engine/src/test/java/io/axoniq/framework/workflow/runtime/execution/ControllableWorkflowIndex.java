/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A {@link WorkflowIndex} whose writes become durable only when the test says so.
 * <p>
 * Loads read what completed writes made durable. Every write is recorded as a {@link PendingWrite} until the test
 * {@link #completeNextWrite() completes} or {@link #failNextWrite(Throwable) fails} it, unless the index
 * {@link #completeWritesImmediately() completes writes immediately}. Initializations take effect immediately, unless
 * the index {@link #failInitializations(Throwable) fails them}.
 *
 * @author Allard Buijze
 */
class ControllableWorkflowIndex implements WorkflowIndex {

    private final InMemoryWorkflowIndex durable = new InMemoryWorkflowIndex();
    private final List<PendingWrite> pending = new ArrayList<>();
    private final List<PendingWrite> issued = new ArrayList<>();
    private final List<Set<String>> initializations = new ArrayList<>();
    private boolean completeImmediately;
    @Nullable
    private Throwable writeFailure;
    @Nullable
    private Throwable initializationFailure;
    @Nullable
    private Function<Segment, CompletableFuture<Optional<Set<String>>>> loads;

    /**
     * Makes this and every later write durable as soon as it is issued, completing pending writes first.
     *
     * @return this index, for chaining
     */
    synchronized ControllableWorkflowIndex completeWritesImmediately() {
        completeImmediately = true;
        writeFailure = null;
        while (!pending.isEmpty()) {
            completeNextWrite();
        }
        return this;
    }

    /**
     * Makes this and every later write fail as soon as it is issued, failing pending writes first.
     *
     * @param failure the failure writes complete with
     * @return this index, for chaining
     */
    synchronized ControllableWorkflowIndex failWritesImmediately(Throwable failure) {
        writeFailure = failure;
        while (!pending.isEmpty()) {
            failNextWrite(failure);
        }
        return this;
    }

    /**
     * Marks the index under the given name initialized, holding the given ids.
     *
     * @param indexName          the name of the index
     * @param runningWorkflowIds the ids the index holds
     * @return this index, for chaining
     */
    synchronized ControllableWorkflowIndex initialized(String indexName, String... runningWorkflowIds) {
        durable.initialize(indexName, Set.of(runningWorkflowIds));
        return this;
    }

    /**
     * Makes every later initialization fail with the given {@code failure}.
     *
     * @param failure the failure initializations complete with
     * @return this index, for chaining
     */
    synchronized ControllableWorkflowIndex failInitializations(Throwable failure) {
        initializationFailure = failure;
        return this;
    }

    /**
     * Makes every later load complete with the given {@code failure}.
     *
     * @param failure the failure loads complete with
     * @return this index, for chaining
     */
    synchronized ControllableWorkflowIndex failLoads(Throwable failure) {
        loads = segment -> CompletableFuture.failedFuture(failure);
        return this;
    }

    /**
     * Makes every later load return a future that never completes.
     *
     * @return this index, for chaining
     */
    synchronized ControllableWorkflowIndex stallLoads() {
        loads = segment -> new CompletableFuture<>();
        return this;
    }

    @Override
    public synchronized CompletableFuture<Optional<Set<String>>> load(String indexName, Segment segment) {
        return loads != null ? loads.apply(segment) : durable.load(indexName, segment);
    }

    @Override
    public CompletableFuture<Void> store(String indexName, IndexDelta delta) {
        return record(indexName, delta, null);
    }

    @Override
    public CompletableFuture<Void> store(String indexName, IndexDelta delta, ProcessingContext context) {
        return record(indexName, delta, context);
    }

    private synchronized CompletableFuture<Void> record(String indexName,
                                                       IndexDelta delta,
                                                       @Nullable ProcessingContext context) {
        var write = new PendingWrite(indexName, delta, context, new CompletableFuture<>());
        issued.add(write);
        if (writeFailure != null) {
            write.done().completeExceptionally(writeFailure);
        } else if (completeImmediately) {
            durable.store(indexName, delta);
            write.done().complete(null);
        } else {
            pending.add(write);
        }
        return write.done();
    }

    @Override
    public synchronized CompletableFuture<Void> initialize(String indexName, Set<String> runningWorkflowIds) {
        initializations.add(Set.copyOf(runningWorkflowIds));
        if (initializationFailure != null) {
            return CompletableFuture.failedFuture(initializationFailure);
        }
        return durable.initialize(indexName, runningWorkflowIds);
    }

    /**
     * Makes the oldest pending write durable and completes its future.
     *
     * @return the completed write
     */
    PendingWrite completeNextWrite() {
        PendingWrite write;
        synchronized (this) {
            write = pending.removeFirst();
            durable.store(write.indexName(), write.delta());
        }
        write.done().complete(null);
        return write;
    }

    /**
     * Fails the oldest pending write without making it durable.
     *
     * @param failure the failure to complete the write with
     * @return the failed write
     */
    PendingWrite failNextWrite(Throwable failure) {
        PendingWrite write;
        synchronized (this) {
            write = pending.removeFirst();
        }
        write.done().completeExceptionally(failure);
        return write;
    }

    /**
     * Returns the writes that are neither completed nor failed yet, oldest first.
     *
     * @return the pending writes
     */
    synchronized List<PendingWrite> pendingWrites() {
        return List.copyOf(pending);
    }

    /**
     * Returns every write issued to this index, oldest first.
     *
     * @return the issued writes
     */
    synchronized List<PendingWrite> issuedWrites() {
        return List.copyOf(issued);
    }

    /**
     * Returns the ids of every initialization issued to this index, oldest first.
     *
     * @return the issued initializations
     */
    synchronized List<Set<String>> initializations() {
        return List.copyOf(initializations);
    }

    /**
     * A write issued to the index.
     *
     * @param indexName the index written to
     * @param delta     the written changes
     * @param context   the context the write was issued within, or {@code null} for a write of its own
     * @param done      the future the index returned for the write
     */
    record PendingWrite(String indexName,
                        IndexDelta delta,
                        @Nullable ProcessingContext context,
                        CompletableFuture<Void> done) {

    }
}
