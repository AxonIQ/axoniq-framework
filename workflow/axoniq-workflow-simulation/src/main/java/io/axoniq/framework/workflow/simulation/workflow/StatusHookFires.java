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

import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Registry of status-change <em>hook fires</em> keyed by {@code workflowId + "/" + status}, used to make
 * lifecycle-hook re-firing observable — the lifecycle-hook analogue of {@link CountingEffects} (which counts
 * {@code execute}-action side effects for INV-6 / F-0).
 * <p>
 * A workflow status-change hook (a {@code @WorkflowStatusChangedHandler} / the test
 * {@code .registerWorkflowStatusChangeListener(status, listener)} registration) is a <strong>user side effect</strong>:
 * it runs application code when the instance transitions to a status (STARTED, COMPLETED, ...). INVARIANTS.md INV-17
 * ({@code StatusHookFiresOncePerStatus}) asks whether that hook fires <em>exactly once</em> per status transition for an
 * instance — including across a crash + replay that re-runs the body and re-drives the engine's event evolution. To
 * observe that, the harness registers a counting {@link io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener
 * status-change listener} that routes every fire through {@link #record(String, WorkflowStatus)}, which bumps a counter.
 * <p>
 * Crucially these counters live <em>outside</em> the engine's durable event log and are deliberately
 * <strong>kept across a simulated crash</strong> — exactly like a real external hook effect (a notification already
 * sent) that the engine cannot roll back. They are therefore <strong>not</strong> reset when an
 * {@code io.axoniq.framework.workflow.simulation.harness.EngineInstance} crashes and recovers; only an explicit {@link #reset()}
 * (a fresh simulation run) clears them. This mirrors {@link CountingEffects} so the same crash-survival reasoning that
 * makes F-0 effect duplication observable also makes lifecycle-hook re-firing observable.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class StatusHookFires {

    private final Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();

    /**
     * Records that the status-change hook for {@code (workflowId, status)} fired once, returning the new count.
     *
     * @param workflowId the workflow instance whose hook fired.
     * @param status     the status the hook fired for.
     * @return the total number of times this hook has fired across the whole simulation (including across crashes).
     */
    public int record(String workflowId, WorkflowStatus status) {
        return counters.computeIfAbsent(key(workflowId, status), k -> new AtomicInteger()).incrementAndGet();
    }

    /**
     * Returns how many times the status-change hook for {@code (workflowId, status)} has fired so far.
     *
     * @param workflowId the workflow instance.
     * @param status     the status.
     * @return the fire count (0 if it never fired).
     */
    public int count(String workflowId, WorkflowStatus status) {
        var counter = counters.get(key(workflowId, status));
        return counter == null ? 0 : counter.get();
    }

    /**
     * Returns an immutable snapshot of every recorded fire count, keyed by {@code workflowId/status}.
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

        private static String key(String workflowId, WorkflowStatus status) {
        return workflowId + "/" + status;
    }
}
