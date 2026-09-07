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
 * Registry of external side-effect counters keyed by {@code workflowId + "/" + stepName}, used to make effect
 * duplication observable.
 * <p>
 * An {@code execute} action's side effect (the {@code action.apply(...)} body) is the workflow's contact with the
 * outside world (sending a payment, calling a service). In the engine this is at-least-once, not at-most-once
 * (INVARIANTS.md INV-6 / finding F-0): a crash after the action ran but before its {@code COMPLETED} event commits
 * re-runs the action on replay. To observe that, the harness routes every {@code execute} body of its test workflows
 * through {@link #record(String, String)}, which bumps a counter.
 * <p>
 * Crucially these counters live <em>outside</em> the engine's durable event log and are deliberately
 * <strong>kept across a simulated crash</strong> — exactly like a real external effect (a charge already sent to a
 * payment provider) that the engine cannot roll back. They are therefore <strong>not</strong> reset when an
 * {@code EngineInstance} crashes and recovers; only an explicit {@link #reset()} (a fresh simulation run) clears them.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class CountingEffects {

    private final Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();

    /**
     * Records that the external side effect for {@code (workflowId, stepName)} ran once, returning the new count.
     *
     * @param workflowId the workflow instance whose step ran.
     * @param stepName   the step whose side effect ran.
     * @return the total number of times this effect has run across the whole simulation (including across crashes).
     */
    public int record(String workflowId, String stepName) {
        return counters.computeIfAbsent(key(workflowId, stepName), k -> new AtomicInteger()).incrementAndGet();
    }

    /**
     * Returns how many times the side effect for {@code (workflowId, stepName)} has run so far.
     *
     * @param workflowId the workflow instance.
     * @param stepName   the step.
     * @return the effect count (0 if it never ran).
     */
    public int count(String workflowId, String stepName) {
        var counter = counters.get(key(workflowId, stepName));
        return counter == null ? 0 : counter.get();
    }

    /**
     * Returns an immutable snapshot of every recorded effect count, keyed by {@code workflowId/stepName}.
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
