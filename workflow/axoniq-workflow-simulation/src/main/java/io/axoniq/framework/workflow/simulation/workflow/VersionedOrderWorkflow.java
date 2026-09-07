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
 * Two coexisting versions of one logical workflow, used to exercise INVARIANTS.md INV-11
 * ({@code VersionRoutingSound}). Both versions share the same {@link #WORKFLOW_NAME} and start event
 * ({@code VersionedOrderRequestedEvent}) but are registered under different {@code workflowVersion}s — the
 * multi-version registration the runtime supports (axon-flow-workflow skill §6; ADR-005), mirroring the
 * {@code MultiVersionRoutingDeclarativeTest} example (a v1 + v2 body sharing name/start event).
 * <p>
 * Each version body runs a shared first step ({@link #STEP_RESERVE_INVENTORY}, a counting side effect so there is a
 * real recorded step after the start) and then a <strong>version-distinguishing</strong> step
 * ({@link #STEP_CHARGE_V1} for v1, {@link #STEP_PROCESS_V2} for v2). Both bodies are pure {@code execute} sequences with
 * no external wait, so the instance reaches a terminal (COMPLETED) status on its own — keeping the harness's liveness
 * assertion simple while still driving the version-routing path through the crash/restart/reorder faults.
 * <p>
 * Because a fresh start spawns at the <em>highest</em> registered version (here {@link #VERSION_V2}), a started instance
 * runs the v2 body and every committed event it emits carries {@link #VERSION_V2} on its {@code MessageType.version()}
 * (sourced from {@code WorkflowContext.workflowVersion()} via {@code EventMessageUtils}); the v1 body's
 * {@link #STEP_CHARGE_V1} must never appear and no event may carry {@link #VERSION_V1}. That single-version-per-instance
 * property — and the highest-version spawn — is exactly what {@code Invariants.assertVersionRoutingSound} checks.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class VersionedOrderWorkflow {

    /**
     * Logical workflow name, stable and shared across both registered versions (multi-version registration keys on
     * {@code (workflowName, startEvent, idProperty)}).
     */
    public static final String WORKFLOW_NAME = "VersionedOrderWorkflow";

    /**
     * Lower registered version. A fresh start must NOT pick this one (the highest version wins); it stays registered
     * for replay routing of any instance recorded at this version.
     */
    public static final String VERSION_V1 = "1.0.0";

    /**
     * Highest registered version. A fresh start spawns here, so every event of a freshly-started instance carries this
     * version.
     */
    public static final String VERSION_V2 = "1.0.1";

    /**
     * Shared first step (a counting side effect), recorded by whichever version body runs.
     */
    public static final String STEP_RESERVE_INVENTORY = "reserveInventory";

    /**
     * v1-only step: present only if the v1 body ran. A freshly-started instance (highest version = v2) must never
     * record this step.
     */
    public static final String STEP_CHARGE_V1 = "chargeV1";

    /**
     * v2-only step: present when the v2 body ran (the expected path for a fresh start at the highest version).
     */
    public static final String STEP_PROCESS_V2 = "processV2";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every {@code execute} body bumps a counter here.
     */
    public VersionedOrderWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The v1 body: shared {@code reserveInventory} step then the v1-only {@code chargeV1} step, then completes.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeV1(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        ctx.awaitExecute(STEP_RESERVE_INVENTORY, Map.of(),
                         (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE_INVENTORY)));
        ctx.awaitExecute(STEP_CHARGE_V1, Map.of(),
                         (pc, payload) -> Map.of("chargedV1", effects.record(workflowId, STEP_CHARGE_V1)));
    }

    /**
     * The v2 body: shared {@code reserveInventory} step then the v2-only {@code processV2} step, then completes. This is
     * the path a fresh start takes (highest registered version).
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeV2(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        ctx.awaitExecute(STEP_RESERVE_INVENTORY, Map.of(),
                         (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE_INVENTORY)));
        ctx.awaitExecute(STEP_PROCESS_V2, Map.of(),
                         (pc, payload) -> Map.of("processedV2", effects.record(workflowId, STEP_PROCESS_V2)));
    }
}
