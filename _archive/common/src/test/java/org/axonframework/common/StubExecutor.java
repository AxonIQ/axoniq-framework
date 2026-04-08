/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.common;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executor;
import org.jspecify.annotations.NonNull;

/**
 * Implementation of the Executor that executes tasks in the calling threads. The {@link #enqueueTasks()} method can be
 * used to queue all tasks instead of executing them immediately, delaying them until the invocation of
 * {@link #runNext()} or {@link #runAll()}.
 * <p>
 * This implementation is not thread safe. It is designed to simulate a multithreaded environment using only a single
 * thread.
 */
public class StubExecutor implements Executor {

    private final Queue<Runnable> tasks = new LinkedList<>();
    private boolean hold = false;

    @Override
    public void execute(@NonNull Runnable task) {
        if (hold) {
            tasks.add(task);
        } else {
            task.run();
        }
    }

    /**
     * Start enqueuing tasks instead of executing them directly. After this call, one must call {@link #runNext()} or
     * {@link #runAll()} to have scheduled tasks executed.
     */
    public void enqueueTasks() {
        this.hold = true;
    }

    /**
     * Runs the next available task, if any, in the calling thread. If the tasks throw an exception, this exception is
     * propagated to the caller
     *
     * @return {@code true} if a task was executed, or {@code false} if there were no tasks to execute
     */
    public boolean runNext() {
        Runnable next = tasks.poll();
        if (next == null) {
            return false;
        }
        next.run();
        return true;
    }

    /**
     * Runs the all available tasks, if any, in the calling thread. If any of the tasks throw an exception, this
     * exception is propagated to the caller and execution of tasks is stopped. Any tasks scheduled during the execution
     * of existing tasks will be executed after previously registered tasks have been completed.
     *
     * @return {@code true} if one or more tasks were executed, or {@code false} if there were no tasks to execute
     */
    public boolean runAll() {
        if (isEmpty()) {
            return false;
        }
        while (!isEmpty()) {
            runNext();
        }
        return true;
    }

    /**
     * Indicates whether the queue of tasks is empty.
     *
     * @return {@code true} if there are no scheduled tasks, or {@code false} is one or more tasks have been scheduled
     */
    public boolean isEmpty() {
        return tasks.isEmpty();
    }
}
