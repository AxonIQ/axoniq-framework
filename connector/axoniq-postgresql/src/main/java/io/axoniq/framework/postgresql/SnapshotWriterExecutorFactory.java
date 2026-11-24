/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.postgresql;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Creates the {@link ExecutorService} that writes the snapshots of a {@link PostgresqlEventStorageEngine}.
 * <p>
 * Snapshots are written in the background, so that storing a snapshot never makes a unit of work wait for a
 * connection. The engine gives the factory the queue that holds the snapshots that are waiting to be written. That
 * queue is bounded: use it for the executor service, so that a snapshot is rejected when too many are waiting, instead
 * of queueing without bound. A snapshot is a cache of state that is already committed, so a rejected snapshot does no
 * harm.
 * <p>
 * The executor service must run its tasks on threads of its own. An executor service that runs a task on the thread
 * that submits it, such as {@code Runnable::run}, would make the unit of work that stores a snapshot wait for the
 * write, and is refused when the engine is created.
 * <p>
 * Every thread of the executor service writes one snapshot at a time, on a connection of its own, so the number of
 * threads is the number of connections that snapshots can use at the same time. With more than one thread, snapshots
 * of the same entity can be written at the same time, or finish out of order. That does no harm: the newest snapshot
 * is the one that is used, and a write that loses such a race fails and is logged.
 * <p>
 * The engine shuts the executor service down when it is closed.
 *
 * @author John Hendrikx
 * @since 5.5.0
 */
@FunctionalInterface
public interface SnapshotWriterExecutorFactory {

    /**
     * Creates the executor service to write the snapshots with.
     *
     * @param queue the bounded queue that holds the snapshots that wait to be written, cannot be {@code null}
     * @return the executor service, never {@code null}
     */
    ExecutorService create(BlockingQueue<Runnable> queue);

    /**
     * Returns the factory that is used when none is configured: an executor service with a single virtual thread, so
     * snapshots are written one at a time, in the order they were handed over.
     *
     * @return the default factory, never {@code null}
     */
    static SnapshotWriterExecutorFactory defaultFactory() {
        return queue -> new ThreadPoolExecutor(
            1, 1, 0L, TimeUnit.MILLISECONDS,
            queue,
            Thread.ofVirtual().name("PG-SnapshotWriter").factory()
        );
    }
}
