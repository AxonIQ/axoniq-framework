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

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.VersioningEdgesSignalEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * One logical workflow registered at <strong>three</strong> coexisting versions, whose single body exercises the
 * versioning edges INVARIANTS.md INV-11 ({@code VersionRoutingSound}) and INV-12 ({@code MigrateVersionContract}) leave
 * uncovered — INV-20 ({@code VersioningEdges}). It deliberately combines, in one body, the three edges:
 * <ol>
 *   <li><strong>Downgrade rejected (§3.7 contract):</strong> after two forward {@code ctx.migrateVersion} bumps the body
 *       attempts a third migration to a version that is <em>not</em> strictly greater than the current recorded version
 *       (a downgrade). {@code VersionDelegate.version} ({@code VersionDelegate.java:117-122}) rejects it by throwing
 *       {@link IllegalArgumentException} <em>before</em> appending any task — so no migration marker is recorded. The
 *       body catches that exception (the documented engine behaviour for an attempted downgrade) and records an
 *       observable {@link #STEP_DOWNGRADE_REJECTED} marker step so the rejection is provably exercised, then continues.
 *       Because the throw is a pure deterministic check before any publish, a replay re-runs the body, re-attempts the
 *       downgrade, throws again, is caught again, and proceeds identically — replay-stable.</li>
 *   <li><strong>Multiple changeIds (extends INV-12 across changeIds):</strong> the body calls {@code ctx.migrateVersion}
 *       twice under two <em>distinct</em>, increasing {@code changeId}s ({@link #CHANGE_ID_1} &rarr; {@link #VERSION_BUMP_1},
 *       then {@link #CHANGE_ID_2} &rarr; {@link #VERSION_BUMP_2}; both strictly above the spawn version since a migration
 *       must move forward). Each marker is recorded at most once, the recorded versions are monotonic non-decreasing
 *       (first-writer-wins, never downgrades), and replaying re-reaches each call as a no-op.</li>
 *   <li><strong>Deeper multi-version routing (extends INV-11 with depth):</strong> the workflow is registered at THREE
 *       versions ({@link #VERSION_LOW}/{@link #VERSION_MID}/{@link #VERSION_HIGH}) sharing one {@code workflowName} and
 *       start event. A fresh start spawns at the <em>highest registered</em> ({@link #VERSION_HIGH}); the body then
 *       migrates the instance's recorded version forward to {@link #VERSION_BUMP_2}. An instance recovered under a
 *       registry whose highest registered version was dropped routes via the 4/5-pass lookup
 *       ({@code WorkflowConfigurationRegistry.resolveDefinitionForReplay}) to the closest registered sibling {@code <=}
 *       its recorded state — never 0 (no body &rarr; stranded), never 2 (two definitions handling one instance). All
 *       three versions register the <em>same</em> body, so whichever definition the lookup routes to replays the
 *       recorded steps cleanly (no drift), letting the routing land on a runnable body observably.</li>
 * </ol>
 * <p>
 * Body shape (identical for every registered version): a shared {@link #STEP_RESERVE_INVENTORY} step, the two forward
 * migrations, the caught downgrade attempt (recording {@link #STEP_DOWNGRADE_REJECTED}), then a {@code waitForEvent}
 * ({@link #STEP_AWAIT_SIGNAL}, correlated on {@code orderId}) and a final {@link #STEP_FINALIZE} step. The wait is the
 * suspension point the routing scenario crashes at (so the resumed final step runs under the closest-sibling definition
 * the lookup routed to); the harness delivers the {@link VersioningEdgesSignalEvent} so the instance self-completes,
 * keeping the fuzz-fold liveness assertion simple.
 * <p>
 * All three {@code migrateVersion} calls are made <strong>directly from the body</strong> (never in a loop or
 * combinator), each at most once per {@code changeId} per invocation, exactly as §3.7 requires.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class VersioningEdgesWorkflow {

    /**
     * Logical workflow name, stable and shared across all three registered versions (multi-version registration keys on
     * {@code (workflowName, startEvent, idProperty)}).
     */
    public static final String WORKFLOW_NAME = "VersioningEdgesWorkflow";

    /**
     * Lowest registered version. A fresh start must NOT pick this one (the highest wins); it stays registered as a
     * closest-sibling routing target for an instance recovered under a reduced registry.
     */
    public static final String VERSION_LOW = "1.0.0";

    /**
     * Middle registered version. As a registered sibling it is the closest registered version {@code <=} an instance
     * recorded (post-migration) at {@link #VERSION_BUMP_2} once the highest registered version ({@link #VERSION_HIGH})
     * is dropped — the deeper closest-sibling routing target the scenario asserts.
     */
    public static final String VERSION_MID = "1.5.0";

    /**
     * Highest <em>registered</em> version: a fresh start spawns here, so the instance's {@code STARTED} event carries
     * this version. The in-body migrations then bump the instance's recorded version forward, past any registered
     * version, to {@link #VERSION_BUMP_2}.
     */
    public static final String VERSION_HIGH = "2.0.0";

    /**
     * The version the first forward migration ({@link #CHANGE_ID_1}) bumps the instance to — strictly greater than the
     * spawn version {@link #VERSION_HIGH} (a migration must move forward), so it is a genuine forward bump that records a
     * marker.
     */
    public static final String VERSION_BUMP_1 = "2.1.0";

    /**
     * The version the second forward migration ({@link #CHANGE_ID_2}) bumps the instance to — strictly greater than
     * {@link #VERSION_BUMP_1}, so the two recorded migration versions are strictly increasing (monotonic). This becomes
     * the instance's recorded {@code workflowDefinitionVersion}; on recovery under a registry whose highest registered
     * version is {@link #VERSION_HIGH} (or lower), the 4/5-pass lookup routes by the closest registered version {@code <=}
     * this recorded state.
     */
    public static final String VERSION_BUMP_2 = "2.2.0";

    /**
     * The downgrade target the body attempts as its third migration: strictly LESS than {@link #VERSION_BUMP_2} (the
     * current recorded version after the two forward bumps), so {@code VersionDelegate} rejects it as a downgrade. It is
     * never recorded.
     */
    public static final String VERSION_DOWNGRADE_ATTEMPT = "2.0.5";

    /**
     * First migration's {@code changeId} — non-blank and stable forever (§3.7). Bumps the instance to
     * {@link #VERSION_BUMP_1}.
     */
    public static final String CHANGE_ID_1 = "ve-bump-1";

    /**
     * Second migration's {@code changeId} — distinct from {@link #CHANGE_ID_1}, increasing the recorded version to
     * {@link #VERSION_BUMP_2}.
     */
    public static final String CHANGE_ID_2 = "ve-bump-2";

    /**
     * Third (downgrade-attempt) migration's {@code changeId}. The engine rejects the downgrade by throwing, so NO
     * migration marker is ever recorded for this {@code changeId} — the absence of a marker for it is exactly what the
     * INV-20 assertion checks (a downgrade was attempted but never recorded).
     */
    public static final String CHANGE_ID_DOWNGRADE = "ve-downgrade";

    /**
     * Shared first step (a counting side effect), recorded before any migration.
     */
    public static final String STEP_RESERVE_INVENTORY = "reserveInventory";

    /**
     * The observable marker step the body records in the {@code catch} branch after the downgrade migration throws — so
     * the downgrade-rejection is provably exercised (non-vacuous), not silently skipped.
     */
    public static final String STEP_DOWNGRADE_REJECTED = "downgradeRejected";

    /**
     * The wait step (correlated on {@code orderId}) the routing scenario suspends the instance at while it is recorded
     * at the highest version, so the recovered, closest-sibling-routed body runs the final step on resume. The harness
     * delivers the matching {@link VersioningEdgesSignalEvent} so a fuzz-folded instance self-completes.
     */
    public static final String STEP_AWAIT_SIGNAL = "awaitSignal";

    /**
     * The final step, recorded after the wait completes. Its presence after a crash + recovery under a registry missing
     * the instance's exact recorded version proves the closest-sibling lookup routed to a runnable body (never 0).
     */
    public static final String STEP_FINALIZE = "finalize";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every {@code execute} body bumps a counter here.
     */
    public VersioningEdgesWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The single body, registered identically for every version. Records {@code reserveInventory}, performs the two
     * forward migrations under distinct {@code changeId}s, attempts (and catches) the rejected downgrade migration,
     * waits for the correlated signal, then records {@code finalize} and completes.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");

        ctx.awaitExecute(STEP_RESERVE_INVENTORY, Map.of(),
                         (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE_INVENTORY)));

        // Two forward migrations under DISTINCT changeIds, each at most once per invocation (extends INV-12 across
        // changeIds): the recorded version moves from the spawn version 2.0.0 -> 2.1.0 -> 2.2.0, strictly increasing.
        // (The targets are ABOVE the highest-registered spawn version because a migration must move forward.)
        ctx.migrateVersion(CHANGE_ID_1, VERSION_BUMP_1);
        ctx.migrateVersion(CHANGE_ID_2, VERSION_BUMP_2);

        // Downgrade attempt: VERSION_DOWNGRADE_ATTEMPT (2.0.5) is NOT strictly greater than the current recorded version
        // (2.2.0), so VersionDelegate.version throws IllegalArgumentException BEFORE appending any task — the marker is
        // never recorded. Catching it (the engine's documented downgrade behaviour) lets the body record an observable
        // proof step and continue. Deterministic + replay-stable: the throw is a pure check, so a replay re-attempts,
        // re-throws, is re-caught, and proceeds identically. The migrateVersion call stays directly in the body (§3.7).
        boolean downgradeRejected;
        try {
            ctx.migrateVersion(CHANGE_ID_DOWNGRADE, VERSION_DOWNGRADE_ATTEMPT);
            downgradeRejected = false; // unreachable unless the engine wrongly ACCEPTED the downgrade — an INV-20 break.
        } catch (IllegalArgumentException expected) {
            downgradeRejected = true;
        }
        // Record the rejection observably. The boolean is always true here (the engine rejects the downgrade); recording
        // it makes the rejection a committed, content-checkable fact rather than an invisible no-op. If the engine ever
        // ACCEPTED the downgrade, downgradeRejected would be false AND a marker for CHANGE_ID_DOWNGRADE would appear in
        // the log carrying a downgraded version — both of which the INV-20 assertion flags as a finding.
        final boolean rejected = downgradeRejected;
        ctx.awaitExecute(STEP_DOWNGRADE_REJECTED, Map.of(),
                         (pc, payload) -> Map.of("downgradeRejected", rejected,
                                                 "n", effects.record(workflowId, STEP_DOWNGRADE_REJECTED)));

        // Suspension point: the routing scenario crashes here (instance recorded at the highest version) and recovers
        // under a registry whose highest version was dropped, so the resumed finalize step runs under the closest
        // sibling the 4/5-pass lookup routes to. The harness delivers the matching signal so the instance self-completes.
        ctx.awaitEvent(STEP_AWAIT_SIGNAL, VersioningEdgesSignalEvent.class,
                       associate(payloadProperty("orderId"), equalsTo(orderId)),
                       step -> step.timeout(Duration.ofDays(365)));

        ctx.awaitExecute(STEP_FINALIZE, Map.of(),
                         (pc, payload) -> Map.of("finalized", effects.record(workflowId, STEP_FINALIZE)));
    }
}
