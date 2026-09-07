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
package io.axoniq.framework.workflow.simulation;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.NoLostPayloadWritesScenario;
import io.axoniq.framework.workflow.simulation.workflow.PayloadOrderWorkflow;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises INVARIANTS.md INV-13 ({@code NoLostPayloadWrites}): for a workflow instance, the final committed payload
 * reflects every committed step's recorded contribution — no committed payload write is lost across crashes/replays
 * (modulo an intended overwrite by a later step on the same key), and replaying the same history rebuilds the identical
 * payload.
 * <p>
 * The first test drives the real engine through {@link PayloadOrderWorkflow}: a fresh start writes a distinct payload
 * key per step (three {@code execute} + {@code CombineGlobalAndLocalPayloadReducer} merges then one {@code modifyPayload}
 * replace), and a crash + replay rebuilds the identical payload with all four contributions present. The remaining
 * tests are assertion pins proving {@link Invariants#assertNoLostPayloadWrites} is correct and not trivial: a payload
 * reflecting every committed contribution passes; a final payload missing a committed step's contribution (with no
 * later same-key overwrite) throws; an intended overwrite by a later same-key {@code combine} write passes; a
 * {@code local_only} replace that drops a prior key is tolerated (the replace is a later same-key overwrite); the
 * cross-instance interleaving of the global log (the F-2 surface, asserted per {@code workflowId}) is tolerated; an
 * instance not yet projected (absent actual payload) is skipped; and non-payload-prefix instances are skipped.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv13NoLostPayloadWritesTest {

    private static final String PREFIX = "payload-";
    private static final String INV = PayloadOrderWorkflow.KEY_INVENTORY_RESERVED; // inventoryReserved
    private static final String PAY = PayloadOrderWorkflow.KEY_PAYMENT_CHARGED;    // paymentCharged
    private static final String SHIP = PayloadOrderWorkflow.KEY_SHIPMENT_RECORDED; // shipmentRecorded
    private static final String FIN = PayloadOrderWorkflow.KEY_ORDER_FINALIZED;    // orderFinalized

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void payloadWrites_allPresentInFinalPayload_stableAcrossReplay() {
        NoLostPayloadWritesScenario.Outcome outcome = NoLostPayloadWritesScenario.run(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("PayloadOrderWorkflow must reach a terminal (COMPLETED) workflow status")
                .isTrue();
        // Every step's distinct payload contribution must be present in the engine's reconstructed final payload.
        assertThat(outcome.payloadBeforeCrash())
                .as("NoLostPayloadWrites: the final payload reflects every committed step's contribution")
                .containsKeys(INV, PAY, SHIP, FIN);
        // Stable across replay: a crash + replay rebuilds the IDENTICAL payload — no committed write lost or dropped.
        assertThat(outcome.payloadAfterCrash())
                .as("NoLostPayloadWrites: replaying the same history rebuilds the identical payload")
                .isEqualTo(outcome.payloadBeforeCrash())
                .containsKeys(INV, PAY, SHIP, FIN);
    }

    @Test
    void assertNoLostPayloadWrites_passesWhenEveryContributionIsReflected() {
        // Three combine writes + one local_only replace that preserves them and adds a fourth — exactly the workflow's
        // shape. The engine's actual payload reflects all four, so no write was lost.
        List<EventMessage> log = List.of(
                started("payload-wf0", Map.of("orderId", "payload-wf0")),
                combine("payload-wf0", PayloadOrderWorkflow.STEP_RESERVE_INVENTORY, Map.of(INV, true)),
                combine("payload-wf0", PayloadOrderWorkflow.STEP_CHARGE_PAYMENT, Map.of(PAY, "charged")),
                combine("payload-wf0", PayloadOrderWorkflow.STEP_RECORD_SHIPMENT, Map.of(SHIP, "shipped")),
                replace("payload-wf0", PayloadOrderWorkflow.STEP_FINALIZE_ORDER,
                        Map.of("orderId", "payload-wf0", INV, true, PAY, "charged", SHIP, "shipped", FIN, true)));
        Map<String, Map<String, Object>> actual = Map.of(
                "payload-wf0", Map.of("orderId", "payload-wf0", INV, true, PAY, "charged", SHIP, "shipped", FIN, true));

        assertThatCode(() -> Invariants.assertNoLostPayloadWrites(log, PREFIX, actual))
                .as("a payload reflecting every committed contribution is sound")
                .doesNotThrowAnyException();
    }

    @Test
    void assertNoLostPayloadWrites_detectsLostContribution() {
        // The genuine break: chargePayment committed its paymentCharged contribution, but the engine's reconstructed
        // final payload does NOT reflect it and no later step overwrote that key — a committed payload write was lost.
        List<EventMessage> log = List.of(
                combine("payload-wf0", PayloadOrderWorkflow.STEP_RESERVE_INVENTORY, Map.of(INV, true)),
                combine("payload-wf0", PayloadOrderWorkflow.STEP_CHARGE_PAYMENT, Map.of(PAY, "charged")),
                combine("payload-wf0", PayloadOrderWorkflow.STEP_RECORD_SHIPMENT, Map.of(SHIP, "shipped")));
        // paymentCharged is missing from the engine's payload — and nothing overwrote it.
        Map<String, Map<String, Object>> actual = Map.of(
                "payload-wf0", Map.of(INV, true, SHIP, "shipped"));

        assertThatThrownBy(() -> Invariants.assertNoLostPayloadWrites(log, PREFIX, actual))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("NoLostPayloadWrites")
                .hasMessageContaining("payload-wf0")
                .hasMessageContaining(PAY);
    }

    @Test
    void assertNoLostPayloadWrites_toleratesIntendedOverwriteOnSameKey() {
        // reserveInventory writes inventoryReserved=1, then a later step overwrites it to inventoryReserved=2. The
        // earlier write is intentionally shadowed by the later same-key write, so the final payload carrying only the
        // later value (=2) is NOT a lost write.
        List<EventMessage> log = List.of(
                combine("payload-wf0", PayloadOrderWorkflow.STEP_RESERVE_INVENTORY, Map.of(INV, 1)),
                combine("payload-wf0", "reReserve", Map.of(INV, 2)));
        Map<String, Map<String, Object>> actual = Map.of("payload-wf0", Map.of(INV, 2));

        assertThatCode(() -> Invariants.assertNoLostPayloadWrites(log, PREFIX, actual))
                .as("a later same-key write is an intended overwrite, not a lost write")
                .doesNotThrowAnyException();
    }

    @Test
    void assertNoLostPayloadWrites_toleratesLocalOnlyReplaceDroppingAPriorKey() {
        // reserveInventory writes inventoryReserved, then a local_only replace rewrites the whole payload WITHOUT it
        // (keeping only paymentCharged). The replace is a deliberate whole-payload overwrite that decides every key's
        // fate, so dropping inventoryReserved is an intended overwrite — tolerated, not a lost write.
        List<EventMessage> log = List.of(
                combine("payload-wf0", PayloadOrderWorkflow.STEP_RESERVE_INVENTORY, Map.of(INV, true)),
                replace("payload-wf0", PayloadOrderWorkflow.STEP_FINALIZE_ORDER, Map.of(PAY, "charged")));
        Map<String, Map<String, Object>> actual = Map.of("payload-wf0", Map.of(PAY, "charged"));

        assertThatCode(() -> Invariants.assertNoLostPayloadWrites(log, PREFIX, actual))
                .as("a local_only replace dropping a prior key is an intended whole-payload overwrite")
                .doesNotThrowAnyException();
    }

    @Test
    void assertNoLostPayloadWrites_ignoresGlobalOnlyResults() {
        // A global_only execute step discards its result (it contributes nothing), so a final payload that does NOT
        // carry that step's result is correct — not a lost write.
        List<EventMessage> log = List.of(
                combine("payload-wf0", PayloadOrderWorkflow.STEP_RESERVE_INVENTORY, Map.of(INV, true)),
                globalOnly("payload-wf0", "transientCompute", Map.of("discarded", "value")));
        Map<String, Map<String, Object>> actual = Map.of("payload-wf0", Map.of(INV, true));

        assertThatCode(() -> Invariants.assertNoLostPayloadWrites(log, PREFIX, actual))
                .as("a global_only step's discarded result is not a payload contribution")
                .doesNotThrowAnyException();
    }

    @Test
    void assertNoLostPayloadWrites_toleratesCrossInstancePayloads() {
        // Two INDEPENDENT payload instances, each with its own contribution reflected in its own actual payload,
        // interleaved in the global log. INV-13 is per-instance, so wf1's write is not "missing from wf0" — must pass.
        List<EventMessage> log = List.of(
                combine("payload-wf0", PayloadOrderWorkflow.STEP_RESERVE_INVENTORY, Map.of(INV, true)),
                combine("payload-wf1", PayloadOrderWorkflow.STEP_RESERVE_INVENTORY, Map.of(INV, true)),
                combine("payload-wf0", PayloadOrderWorkflow.STEP_CHARGE_PAYMENT, Map.of(PAY, "charged")));
        Map<String, Map<String, Object>> actual = Map.of(
                "payload-wf0", Map.of(INV, true, PAY, "charged"),
                "payload-wf1", Map.of(INV, true));

        assertThatCode(() -> Invariants.assertNoLostPayloadWrites(log, PREFIX, actual))
                .as("independent instances' payloads are checked per-workflowId")
                .doesNotThrowAnyException();
    }

    @Test
    void assertNoLostPayloadWrites_skipsInstanceWithNoReconstructedPayloadYet() {
        // The instance has committed a contribution but the engine has not reconstructed a payload for it yet (absent
        // from the actual map) — there is nothing to cross-check, so it must be skipped (no false positive mid-run).
        List<EventMessage> log = List.of(
                combine("payload-wf0", PayloadOrderWorkflow.STEP_RESERVE_INVENTORY, Map.of(INV, true)));

        assertThatCode(() -> Invariants.assertNoLostPayloadWrites(log, PREFIX, Map.of()))
                .as("an instance with no reconstructed payload yet is skipped (no cross-check possible)")
                .doesNotThrowAnyException();
    }

    @Test
    void assertNoLostPayloadWrites_skipsNonPayloadInstances() {
        // A different-prefix instance is not constrained by INV-13 — even a (hypothetical) missing contribution on it
        // must be skipped, not flagged (INV-13 is scoped to the payload workflow's id prefix).
        List<EventMessage> log = List.of(
                combine("order-wf0", PayloadOrderWorkflow.STEP_RESERVE_INVENTORY, Map.of(INV, true)));
        Map<String, Map<String, Object>> actual = Map.of("order-wf0", Map.of());

        assertThatCode(() -> Invariants.assertNoLostPayloadWrites(log, PREFIX, actual))
                .as("non-payload (order-) instances are out of INV-13's scope and are skipped")
                .doesNotThrowAnyException();
    }

    @Test
    void rebuildPayload_foldsCombineThenLocalOnlyReplace() {
        // The committed-log fold helper mirrors evolvePayload: combine merges, local_only replaces. Two combine merges
        // then a local_only replace that adds a key while preserving the earlier ones yields all three keys.
        List<EventMessage> log = List.of(
                combine("payload-wf0", PayloadOrderWorkflow.STEP_RESERVE_INVENTORY, Map.of(INV, true)),
                combine("payload-wf0", PayloadOrderWorkflow.STEP_CHARGE_PAYMENT, Map.of(PAY, "charged")),
                replace("payload-wf0", PayloadOrderWorkflow.STEP_FINALIZE_ORDER,
                        Map.of(INV, true, PAY, "charged", FIN, true)));

        assertThat(Invariants.rebuildPayload(log, "payload-wf0"))
                .as("rebuildPayload folds combine (merge) then local_only (replace) like the engine")
                .containsEntry(INV, true)
                .containsEntry(PAY, "charged")
                .containsEntry(FIN, true);
    }

    private static EventMessage started(String workflowId, Map<String, Object> payload) {
        // The workflow STARTED event carries the combine_local_and_global reducer (it seeds the initial payload).
        Metadata metadata = MetadataUtils.create(workflowId,
                                                 io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus.STARTED)
                                         .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD,
                                              CombineGlobalAndLocalPayloadReducer.NAME);
        return new GenericEventMessage(new MessageType("workflow"), payload, metadata);
    }

    private static EventMessage combine(String workflowId, String stepName, Map<String, Object> result) {
        return payloadStep(workflowId, stepName, result, CombineGlobalAndLocalPayloadReducer.NAME);
    }

    private static EventMessage replace(String workflowId, String stepName, Map<String, Object> newPayload) {
        return payloadStep(workflowId, stepName, newPayload, LocalOnlyPayloadReducer.NAME);
    }

    private static EventMessage globalOnly(String workflowId, String stepName, Map<String, Object> result) {
        return payloadStep(workflowId, stepName, result, GlobalOnlyPayloadReducer.NAME);
    }

    /**
     * A COMPLETED step event carrying the {@code result} payload and the named {@code payloadReducer} metadata key —
     * exactly what {@code EventMessageUtils.completedStep} emits for a payload-bearing step.
     */
    private static EventMessage payloadStep(String workflowId, String stepName, Map<String, Object> result,
                                            String reducerName) {
        Metadata metadata = MetadataUtils.create(workflowId, stepName, StepStatus.COMPLETED)
                                         .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD, reducerName);
        return new GenericEventMessage(new MessageType(stepName), result, metadata);
    }
}
