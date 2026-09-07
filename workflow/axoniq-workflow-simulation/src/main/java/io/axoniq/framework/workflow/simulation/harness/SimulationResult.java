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
package io.axoniq.framework.workflow.simulation.harness;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.Map;

/**
 * Outcome of one deterministic simulation run, used to assert seed reproducibility and to surface what the run
 * observed (committed log, effect counts, fault trace, whether F-0 effect-duplication occurred).
 *
 * @param seed                the seed that produced this run.
 * @param committedLog        the final committed workflow event log (the durable substrate after the run).
 * @param logFingerprint      a stable, comparable rendering of {@code committedLog} (for same-seed equality checks).
 * @param effectCounts        external side-effect counts keyed by {@code workflowId/stepName}.
 * @param faultTrace          ordered human-readable trace of injected faults (the seed→fault-sequence).
 * @param observedF0Duplicate {@code true} if any counted effect ran more than once (F-0 reproduced).
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public record SimulationResult(
        long seed,
        List<EventMessage> committedLog,
        List<String> logFingerprint,
        Map<String, Integer> effectCounts,
        List<String> faultTrace,
        boolean observedF0Duplicate,
        java.time.Duration virtualElapsed
) {

    /**
     * Compatibility constructor — pre-Phase-3 shape (no virtual-time accounting).
     */
    public SimulationResult(long seed,
                            List<EventMessage> committedLog,
                            List<String> logFingerprint,
                            Map<String, Integer> effectCounts,
                            List<String> faultTrace,
                            boolean observedF0Duplicate) {
        this(seed, committedLog, logFingerprint, effectCounts, faultTrace, observedF0Duplicate,
             java.time.Duration.ZERO);
    }

    /**
     * Renders the committed log as a GLOBAL (whole-log, cross-instance order) fingerprint. Unlike
     * {@link #logFingerprint()} (per-workflow content, the harness's determinism contract), global equality only
     * holds where the world is globally deterministic — measured today for single-instance worlds under the Phase-2
     * FIFO carrier; multi-instance global order still varies with processor delivery timing (Phase 2' boundary).
     *
     * @return one line per committed event, in global append order.
     */
        public List<String> globalFingerprint() {
        return committedLog.stream()
                           .map(e -> io.axoniq.framework.workflow.runtime.util.MetadataUtils.getWorkflowId(e.metadata())
                                   + "|" + e.type().qualifiedName()
                                   + "|" + io.axoniq.framework.workflow.runtime.util.MetadataUtils.getStepName(e.metadata())
                                   + "|" + io.axoniq.framework.workflow.runtime.util.MetadataUtils
                                             .getWorkflowStatus(e.metadata()).map(Enum::name).orElse("-"))
                           .toList();
    }

    /**
     * Returns the maximum number of times any single effect ran (1 = no duplication; ≥2 = F-0 observed).
     *
     * @return the maximum effect count across all steps.
     */
    public int maxEffectCount() {
        return effectCounts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
    }
}
