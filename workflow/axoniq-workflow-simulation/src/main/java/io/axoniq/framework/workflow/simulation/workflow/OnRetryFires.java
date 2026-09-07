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
package io.axoniq.framework.workflow.simulation.workflow;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Registry of {@code RetryPolicy.onRetry(...)} <em>handler fires</em> keyed by {@code workflowId + "/" + stepName}, used
 * to make {@code onRetry} re-firing on replay observable — the retry-handler analogue of {@link CountingEffects} (which
 * counts {@code execute}-action side effects for INV-6 / F-0) and {@link StatusHookFires} (status-change hooks for
 * INV-17).
 * <p>
 * An {@code onRetry} handler ({@code RetryPolicy.onRetry(ctx -> ...)}) is a <strong>user side effect</strong>: it runs
 * application code (log/metric/alert) once per <em>actual retry decision</em>, immediately before the engine publishes
 * the {@code RETRYING} event ({@code RetryableExecuteDelegate.handleAttemptFailure} calls
 * {@code retryPolicy.onRetryHandler().onRetry(retryContext)} only on the <em>live</em> failure path, never on the
 * crash-recovery resume path that re-schedules from the persisted {@code RETRYING} state). INVARIANTS.md INV-21
 * ({@code RetryTimingAndExhaustionEdges}) asks whether that handler fires <em>exactly once per actual retry decision</em>
 * and is <strong>not</strong> re-invoked when a crash + replay re-reaches the already-recorded {@code RETRYING} steps —
 * the same at-most-once / no-re-fire contract INV-6 (effects) and INV-17 (status hooks) probe. The {@code RetryHandler}
 * Javadoc itself states it is "Not called during replay", which this counter checks against the running engine.
 * <p>
 * Crucially these counters live <em>outside</em> the engine's durable event log and are deliberately
 * <strong>kept across a simulated crash</strong> — exactly like a real external retry-handler effect (a metric already
 * incremented, an alert already sent) that the engine cannot roll back. They are therefore <strong>not</strong> reset
 * when an {@code io.axoniq.framework.workflow.simulation.harness.EngineInstance} crashes and recovers; only an explicit
 * {@link #reset()} (a fresh simulation run) clears them. This mirrors {@link CountingEffects} / {@link StatusHookFires}
 * so the same crash-survival reasoning that makes F-0 effect duplication observable also makes {@code onRetry} re-firing
 * observable.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class OnRetryFires {

    private final Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();

    /**
     * Records that the {@code onRetry} handler for {@code (workflowId, stepName)} fired once, returning the new count.
     *
     * @param workflowId the workflow instance whose retry handler fired.
     * @param stepName   the step whose retry handler fired.
     * @return the total number of times this handler has fired across the whole simulation (including across crashes).
     */
    public int record(String workflowId, String stepName) {
        return counters.computeIfAbsent(key(workflowId, stepName), k -> new AtomicInteger()).incrementAndGet();
    }

    /**
     * Returns how many times the {@code onRetry} handler for {@code (workflowId, stepName)} has fired so far.
     *
     * @param workflowId the workflow instance.
     * @param stepName   the step.
     * @return the fire count (0 if it never fired).
     */
    public int count(String workflowId, String stepName) {
        var counter = counters.get(key(workflowId, stepName));
        return counter == null ? 0 : counter.get();
    }

    /**
     * Returns an immutable snapshot of every recorded fire count, keyed by {@code workflowId/stepName}.
     *
     * @return a copy of all counters.
     */
        public Map<String, Integer> snapshot() {
        var snapshot = new java.util.TreeMap<String, Integer>();
        counters.forEach((k, v) -> snapshot.put(k, v.get()));
        return Map.copyOf(snapshot);
    }

    /**
     * Clears all counters. Call only between independent simulation runs, never across a crash within a run.
     */
    public void reset() {
        counters.clear();
    }

        private static String key(String workflowId, String stepName) {
        return workflowId + "/" + stepName;
    }
}
