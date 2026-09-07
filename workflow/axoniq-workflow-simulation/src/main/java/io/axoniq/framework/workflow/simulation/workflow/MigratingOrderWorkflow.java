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

import java.util.Map;

/**
 * A single workflow whose body calls {@code ctx.migrateVersion(changeId, newVersion)} to fork its logic mid-flight,
 * used to exercise INVARIANTS.md INV-12 ({@code MigrateVersionContract}). It mirrors the
 * {@code VersionedWorkflowDeclarativeTest} example workflow (axon-flow-workflow skill §7.6 / ADR-005): a fresh
 * deployment introduces a version bump and the body branches on the migration result.
 * <p>
 * Unlike {@link VersionedOrderWorkflow} (INV-11, which registers <em>two definitions</em> of one logical workflow to
 * exercise multi-version <em>routing</em>), this is a <strong>single</strong> definition that performs an
 * <em>in-body</em> version migration via the {@code migrateVersion} primitive. The migration records a marker step
 * whose recorded version lives in the committed log (a {@code COMPLETED} step event carrying the {@code versionChangeId}
 * + {@code version} metadata keys — {@code MetadataUtils.createVersionMigrationStep}), and {@code EventSourcedWorkflowState}
 * pins it first-writer-wins ({@code versions.putIfAbsent}, {@code EventSourcedWorkflowState.java:293}).
 * <p>
 * Body shape: a shared first step ({@link #STEP_RESERVE_INVENTORY}, a counting side effect so there is a real recorded
 * step before the migration), then a single {@code ctx.migrateVersion(}{@link #CHANGE_ID}{@code ,}
 * {@link #MIGRATED_VERSION}{@code )} call — at most once per {@code changeId} per body invocation, never in a loop or
 * combinator (§3.7) — and a branch: a fresh instance takes the migrated branch (records {@link #STEP_PROCESS_V2}), an
 * in-flight instance that already ran past the call under old code stays on the legacy branch (records
 * {@link #STEP_CHARGE_V1}). Both branches are pure {@code execute} sequences with no external wait, so the instance
 * reaches a terminal (COMPLETED) status on its own — keeping the harness's liveness assertion simple while still
 * driving the {@code migrateVersion} record through the crash/restart/reorder faults (exactly where a replay-stability
 * bug would show, since replay re-runs the body and must resolve the same recorded version without re-applying it).
 * <p>
 * Because a fresh start spawns at {@link #INITIAL_VERSION} and immediately migrates to {@link #MIGRATED_VERSION}, every
 * committed event of a freshly-started instance after the migration carries {@link #MIGRATED_VERSION} (the migration
 * bumps {@code workflowDefinitionVersion}), the migration marker is recorded exactly once, and replaying the same
 * history resolves that same recorded version unchanged — exactly what {@code Invariants.assertMigrateVersionContract}
 * checks.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class MigratingOrderWorkflow {

    /**
     * Logical workflow name (single definition; this workflow performs an in-body migration rather than registering
     * multiple definitions).
     */
    public static final String WORKFLOW_NAME = "MigratingOrderWorkflow";

    /**
     * The version a fresh instance starts under (the definition's configured {@code workflowVersion}). The body then
     * migrates forward to {@link #MIGRATED_VERSION}.
     */
    public static final String INITIAL_VERSION = "1.0.0";

    /**
     * The version the body migrates to via {@code ctx.migrateVersion}. Strictly greater than {@link #INITIAL_VERSION}
     * (the {@code migrateVersion} contract rejects downgrades), so the migration is a live forward bump that records a
     * marker step.
     */
    public static final String MIGRATED_VERSION = "1.0.1";

    /**
     * The developer-chosen {@code changeId} for the migration — non-blank and stable forever (§3.7). It is also the
     * recorded marker step's name (the migration step is a {@code COMPLETED} step event with {@code stepName = changeId}).
     */
    public static final String CHANGE_ID = "payment-redesign";

    /**
     * Shared first step (a counting side effect), recorded before the migration regardless of which branch runs.
     */
    public static final String STEP_RESERVE_INVENTORY = "reserveInventory";

    /**
     * Legacy-branch step: recorded only when the migration returns {@code false} (an in-flight instance that already ran
     * past the {@code migrateVersion} call under old code). A freshly-started instance never records this step.
     */
    public static final String STEP_CHARGE_V1 = "chargeV1";

    /**
     * Migrated-branch step: recorded when the migration returns {@code true} (the expected path for a fresh start, which
     * migrates forward to {@link #MIGRATED_VERSION}).
     */
    public static final String STEP_PROCESS_V2 = "processV2";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every {@code execute} body bumps a counter here.
     */
    public MigratingOrderWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The body: a shared {@code reserveInventory} step, then a single {@code ctx.migrateVersion(CHANGE_ID,
     * MIGRATED_VERSION)} call (at most once per {@code changeId} per invocation, never in a loop/combinator), then a
     * branch — the migrated {@code processV2} step when the migration is in effect, the legacy {@code chargeV1} step
     * otherwise. Completes on its own (execute-only, no external wait).
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        ctx.awaitExecute(STEP_RESERVE_INVENTORY, Map.of(),
                         (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE_INVENTORY)));
        if (ctx.migrateVersion(CHANGE_ID, MIGRATED_VERSION)) {
            ctx.awaitExecute(STEP_PROCESS_V2, Map.of(),
                             (pc, payload) -> Map.of("processedV2", effects.record(workflowId, STEP_PROCESS_V2)));
        } else {
            ctx.awaitExecute(STEP_CHARGE_V1, Map.of(),
                             (pc, payload) -> Map.of("chargedV1", effects.record(workflowId, STEP_CHARGE_V1)));
        }
    }
}
