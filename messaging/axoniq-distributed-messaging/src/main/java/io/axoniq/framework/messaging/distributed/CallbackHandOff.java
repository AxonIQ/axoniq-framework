/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.distributed;

import org.axonframework.common.annotation.Internal;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Turns the work a consumer does when a {@link org.axonframework.messaging.core.MessageStream} has something to read
 * into a callback that is safe to register with that stream.
 * <p>
 * A stream calls its callback on the thread of whoever produced the entry or ended the stream. That thread is usually
 * shared with other streams, so a callback must finish in microseconds, and must not read from the stream: reading
 * can involve I/O, and the stream rejects it. The callback returned by this class only schedules the work on another
 * thread, where the consumer can read, and write the entries on to wherever they go, as long as it needs to.
 * <p>
 * Signals that arrive before the scheduled work has started share that one run, as the work reads everything that is
 * available when it starts. A signal that arrives while the work runs schedules the work again, so an entry that
 * arrives after the work last looked is never missed.
 * <p>
 * Internal, as it exists for the connectors of this framework to share.
 *
 * @author John Hendrikx
 * @since 5.5.0
 */
@Internal
public final class CallbackHandOff {

    private CallbackHandOff() {
        // utility class
    }

    /**
     * Returns a callback that runs the given {@code task} on a virtual thread each time the stream signals.
     * <p>
     * See {@link #handOff(Executor, Runnable)} for how signals are combined.
     *
     * @param task the work to run when the stream has something to read, which must not throw an exception
     * @return a callback for {@link org.axonframework.messaging.core.MessageStream#setCallback(Runnable)}, never
     * {@code null}
     * @throws NullPointerException if {@code task} is {@code null}
     */
    public static Runnable handOff(Runnable task) {
        return handOff(Thread::startVirtualThread, task);
    }

    /**
     * Returns a callback that runs the given {@code task} on a thread of the given {@code executor} each time the
     * stream signals.
     * <p>
     * A signal that arrives while a run is scheduled, but has not started, does not schedule another one. A signal that
     * arrives once a run has started does, as that run may already have passed what the signal announces.
     * <p>
     * If the {@code executor} rejects the work, the exception is thrown from the callback, which completes the stream
     * with it, and later signals are scheduled again.
     *
     * @param executor the executor to run the task on, which must not run it on the thread that signals
     * @param task     the work to run when the stream has something to read, which must not throw an exception
     * @return a callback for {@link org.axonframework.messaging.core.MessageStream#setCallback(Runnable)}, never
     * {@code null}
     * @throws NullPointerException if {@code executor} or {@code task} is {@code null}
     */
    public static Runnable handOff(Executor executor, Runnable task) {
        Objects.requireNonNull(executor, "The executor parameter must not be null.");
        Objects.requireNonNull(task, "The task parameter must not be null.");

        AtomicBoolean scheduled = new AtomicBoolean();

        return () -> {
            if (scheduled.compareAndSet(false, true)) {
                try {
                    executor.execute(() -> {
                        scheduled.set(false);  // before the task, so that a signal during the task schedules it again
                        task.run();
                    });
                } catch (RuntimeException e) {
                    scheduled.set(false);

                    throw e;
                }
            }
        };
    }
}
