/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.runtime.test.fakes;

import io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Virtual-time {@link WorkflowScheduler} fake for deterministic testing and simulation.
 * <p>
 * Instead of handing deadlines to a real timer thread, this scheduler queues each scheduled task against its deadline
 * on an internal clock that never advances on its own. Tasks fire only when the test advances virtual time via
 * {@link #advanceBy(Duration)} or {@link #advanceTo(long)}, at which point all tasks whose deadline has been reached
 * run synchronously on the advancing thread, in non-decreasing deadline order (ties run in submission order). This
 * gives a deterministic simulator full control over when step timeouts, wait-for-event timeouts and retry backoff
 * fire.
 * <p>
 * The virtual clock is expected to be advanced in lock-step with the {@link MutableClock} the engine resolves time
 * through, since deadlines handed to {@link #schedule(Instant)} are absolute instants computed against that
 * clock.
 * <p>
 * Submission and advancement are synchronized so the scheduler is safe to share across the engine's worker threads
 * while a test thread advances time. Tasks submitted while time is being advanced are themselves honored if their
 * deadline falls within the same advancement window.
 * <p>
 * Register this in place of the default {@code DefaultWorkflowScheduler} via the configurer:
 * {@code componentRegistry(cr -> cr.registerComponent(WorkflowScheduler.class, cfg -> scheduler))}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class ManualWorkflowScheduler implements WorkflowScheduler {

    private final Object lock = new Object();
    private final List<Scheduled> queue = new ArrayList<>();
    private final AtomicLong sequence = new AtomicLong();
    private long currentMillis;

    /**
     * Creates a scheduler whose virtual clock starts at {@code 0} milliseconds.
     */
    public ManualWorkflowScheduler() {
        this(0L);
    }

    /**
     * Creates a scheduler whose virtual clock starts at the given epoch milliseconds.
     *
     * @param startMillis initial virtual time in milliseconds.
     */
    public ManualWorkflowScheduler(long startMillis) {
        this.currentMillis = startMillis;
    }

        @Override
    public ScheduledTask schedule(Instant deadline) {
        var scheduled = new Scheduled(deadline.toEpochMilli(), sequence.getAndIncrement());
        synchronized (lock) {
            queue.add(scheduled);
        }
        return scheduled;
    }

    /**
     * Advances virtual time by the given duration, firing every task whose deadline is reached.
     *
     * @param delta amount to advance; non-positive values fire only tasks already due at the current time.
     */
    public void advanceBy(Duration delta) {
        advanceTo(currentMillis() + Math.max(0L, delta.toMillis()));
    }

    /**
     * Advances virtual time to the given epoch milliseconds, firing every task whose deadline is reached.
     * <p>
     * Tasks are run in non-decreasing deadline order. Tasks that schedule further tasks during this call are honored
     * if their deadline falls at or before {@code targetMillis}. A target at or before the current time does not move
     * the clock backwards but still fires any tasks already due.
     *
     * @param targetMillis virtual time to advance to, in milliseconds.
     */
    public void advanceTo(long targetMillis) {
        synchronized (lock) {
            if (targetMillis > currentMillis) {
                currentMillis = targetMillis;
            }
            Scheduled next;
            while ((next = pollDue()) != null) {
                next.fire();
            }
        }
    }

    /**
     * Returns the current virtual time in milliseconds.
     *
     * @return current virtual time.
     */
    public long currentMillis() {
        synchronized (lock) {
            return currentMillis;
        }
    }

    /**
     * Returns the number of tasks still queued and not yet due/fired.
     *
     * @return pending task count.
     */
    public int pendingTasks() {
        synchronized (lock) {
            return queue.size();
        }
    }

    private Scheduled pollDue() {
        Scheduled earliest = null;
        for (Scheduled candidate : queue) {
            if (candidate.dueMillis <= currentMillis
                    && (earliest == null || candidate.isBefore(earliest))) {
                earliest = candidate;
            }
        }
        if (earliest != null) {
            queue.remove(earliest);
        }
        return earliest;
    }

    private final class Scheduled implements ScheduledTask {

        private final long dueMillis;
        private final long sequenceNumber;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();

        private Scheduled(long dueMillis, long sequenceNumber) {
            this.dueMillis = dueMillis;
            this.sequenceNumber = sequenceNumber;
        }

                @Override
        public CompletableFuture<Void> completion() {
            return completion;
        }

        @Override
        public void cancel() {
            completion.cancel(false);
            synchronized (lock) {
                queue.remove(this);
            }
        }

        private void fire() {
            if (completion.isDone()) {
                return;
            }
            completion.complete(null);
        }

        private boolean isBefore(Scheduled other) {
            return dueMillis < other.dueMillis
                    || (dueMillis == other.dueMillis && sequenceNumber < other.sequenceNumber);
        }
    }
}
