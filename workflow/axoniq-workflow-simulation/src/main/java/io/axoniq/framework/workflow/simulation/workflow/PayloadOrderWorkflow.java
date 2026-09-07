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
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;

import java.util.HashMap;
import java.util.Map;

/**
 * A workflow whose every step writes a <strong>distinct</strong> key to the workflow payload, used to exercise
 * INVARIANTS.md INV-13 ({@code NoLostPayloadWrites}): the final committed payload must reflect <em>every</em> committed
 * step's recorded contribution — no committed payload write is lost or silently dropped across crashes/replays (modulo
 * an intended overwrite by a later step on the same key).
 * <p>
 * Two payload-write mechanisms are deliberately combined so a lost write is observable on both code paths the engine
 * evolves payload through ({@code EventSourcedWorkflowState#evolvePayload}, applying the COMPLETED step event's named
 * {@code PayloadReducer}):
 * <ul>
 *   <li><strong>{@code execute} + {@link CombineGlobalAndLocalPayloadReducer#INSTANCE}</strong> (axon-flow-workflow
 *       skill §5; mirrors {@code CombineResultIntegrationTest}): the step's result map is <em>merged</em> into the
 *       payload (the {@code combine_local_and_global} reducer keeps every existing key and adds the step's). Each of
 *       {@link #STEP_RESERVE_INVENTORY}, {@link #STEP_CHARGE_PAYMENT} and {@link #STEP_RECORD_SHIPMENT} contributes one
 *       distinct key this way;</li>
 *   <li><strong>{@code modifyPayload}</strong> ({@link SimpleWorkflowContext#awaitModifyPayload}, the
 *       {@code local_only} reducer that <em>replaces</em> the whole payload): {@link #STEP_FINALIZE_ORDER} reads the
 *       current payload and returns it with one additional key, so it adds its own contribution while
 *       <em>preserving</em> the three earlier keys — a {@code local_only} replace that must not drop a prior committed
 *       write.</li>
 * </ul>
 * The body is a pure {@code execute}/{@code modifyPayload} sequence with no external wait, so the instance reaches a
 * terminal (COMPLETED) status on its own — keeping the harness's liveness assertion simple while still driving every
 * payload write through the crash/restart/reorder faults, exactly where a lost write would show (a crash between a
 * step's effect and its COMPLETED commit, or a replay that rebuilds the payload, must not drop an earlier committed
 * key). Determinism (axon-flow-workflow skill §3.3): each contribution is a constant derived only from the step name —
 * the workflow body holds no wall-clock/random/external state — so replaying the same history rebuilds the identical
 * payload, the replay-stability facet INV-13 shares with INV-4 ({@code DeterministicReplay}).
 * <p>
 * The {@code execute} bodies also bump the shared {@link CountingEffects} counter (like the other simulation workflows)
 * so the run still exercises the effect-counting surface; INV-13 itself reads only the committed payload contributions
 * from the event log, never the effect counters.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class PayloadOrderWorkflow {

    /**
     * Logical workflow name (single definition).
     */
    public static final String WORKFLOW_NAME = "PayloadOrderWorkflow";

    /**
     * First payload-writing step ({@code execute} + {@link CombineGlobalAndLocalPayloadReducer#INSTANCE}): merges
     * {@link #KEY_INVENTORY_RESERVED} into the payload.
     */
    public static final String STEP_RESERVE_INVENTORY = "reserveInventory";

    /**
     * Second payload-writing step ({@code execute} + {@link CombineGlobalAndLocalPayloadReducer#INSTANCE}): merges
     * {@link #KEY_PAYMENT_CHARGED} into the payload.
     */
    public static final String STEP_CHARGE_PAYMENT = "chargePayment";

    /**
     * Third payload-writing step ({@code execute} + {@link CombineGlobalAndLocalPayloadReducer#INSTANCE}): merges
     * {@link #KEY_SHIPMENT_RECORDED} into the payload.
     */
    public static final String STEP_RECORD_SHIPMENT = "recordShipment";

    /**
     * Final payload-writing step ({@code modifyPayload}, the {@code local_only} replace): adds
     * {@link #KEY_ORDER_FINALIZED} while preserving every earlier key.
     */
    public static final String STEP_FINALIZE_ORDER = "finalizeOrder";

    /**
     * Distinct payload key contributed by {@link #STEP_RESERVE_INVENTORY}.
     */
    public static final String KEY_INVENTORY_RESERVED = "inventoryReserved";

    /**
     * Distinct payload key contributed by {@link #STEP_CHARGE_PAYMENT}.
     */
    public static final String KEY_PAYMENT_CHARGED = "paymentCharged";

    /**
     * Distinct payload key contributed by {@link #STEP_RECORD_SHIPMENT}.
     */
    public static final String KEY_SHIPMENT_RECORDED = "shipmentRecorded";

    /**
     * Distinct payload key contributed by {@link #STEP_FINALIZE_ORDER}.
     */
    public static final String KEY_ORDER_FINALIZED = "orderFinalized";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every {@code execute} body bumps a counter here.
     */
    public PayloadOrderWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The body: three {@code execute} steps that each merge a distinct key into the payload (via
     * {@link CombineGlobalAndLocalPayloadReducer#INSTANCE}), then one {@code modifyPayload} step that adds a fourth
     * distinct key while preserving the earlier three. Completes on its own (no external wait). Each contribution is a
     * constant value keyed only by the step, so the body is deterministic and a replay rebuilds the identical payload.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();

        ctx.awaitExecute(
                STEP_RESERVE_INVENTORY,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_RESERVE_INVENTORY);
                    return Map.of(KEY_INVENTORY_RESERVED, true);
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));

        ctx.awaitExecute(
                STEP_CHARGE_PAYMENT,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_CHARGE_PAYMENT);
                    return Map.of(KEY_PAYMENT_CHARGED, "charged");
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));

        ctx.awaitExecute(
                STEP_RECORD_SHIPMENT,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_RECORD_SHIPMENT);
                    return Map.of(KEY_SHIPMENT_RECORDED, "shipped");
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));

        // modifyPayload (local_only replace): preserve every existing key and add one more. This is deterministic —
        // it reads only the current payload (rebuilt identically on replay) and adds a constant key — and proves a
        // whole-payload replace must not drop the three earlier committed contributions.
        ctx.awaitModifyPayload(
                STEP_FINALIZE_ORDER,
                currentPayload -> {
                    var merged = new HashMap<String, Object>(currentPayload);
                    merged.put(KEY_ORDER_FINALIZED, true);
                    return merged;
                });
    }
}
