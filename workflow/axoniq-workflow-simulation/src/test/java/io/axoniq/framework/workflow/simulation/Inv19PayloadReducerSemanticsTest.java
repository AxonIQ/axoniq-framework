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
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.PayloadReducerSemanticsScenario;
import io.axoniq.framework.workflow.simulation.workflow.ReducerWorkflow;
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
 * Exercises INVARIANTS.md INV-19 ({@code PayloadReducerSemantics}): each payload reducer produces its documented merge
 * into the workflow payload, and that result is stable across crash/replay — {@code global_only} discards the result,
 * {@code combine_local_and_global} merges it key-by-key, {@code local_only} replaces the whole payload, and the
 * {@code parameterPayloadReducer} governs the step's input view. Extends INV-13 ({@code NoLostPayloadWrites}).
 * <p>
 * The first test drives the real engine through {@link ReducerWorkflow}: a fresh start exercises all three reducers and
 * the reducer EDGE cases, and the engine's reconstructed final payload reflects each one's documented outcome (the
 * combine key present, the {@code global_only} result key absent, the {@code local_only}-dropped prior key absent, the
 * parameter-side combine view {@code true}, the interplay key absent — edge (b) — and the last-writer-wins key carrying
 * the later writer's value — edge (c)); a crash + replay rebuilds the identical payload. A second engine-driven test
 * exercises edge (a) (a {@code combine} step whose result map carries a {@code null} value) and edge (e) (replay-stable
 * with a {@code null} value), characterizing the engine's actual null-under-combine handling against the documented
 * fold. The remaining tests are assertion pins proving {@link Invariants#assertPayloadReducerSemantics} is correct and
 * not trivial: a payload matching the documented reducer fold passes; a {@code global_only} step whose result WRONGLY
 * appears throws; a {@code combine} step whose key is MISSING throws; a {@code local_only} replace that did NOT drop a
 * prior key throws; a value divergence throws; an interplay {@code global_only} write that wrongly appears throws (edge
 * (b)); an earlier same-key writer that wrongly wins throws (edge (c)); a combine that omits a key keeps that key's
 * prior value (edge (d)); a {@code null}-value combine folds to key-present-with-null and a dropped null throws (edge
 * (a)); the per-instance fold is content-based and tolerates a reordered global append (F-2); the cross-instance
 * interleaving of the global log (asserted per {@code workflowId}) is tolerated; an instance not yet projected is
 * skipped; and out-of-scope prefix instances are skipped.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv19PayloadReducerSemanticsTest {

    private static final String PREFIX = "reducer-";
    private static final String SEED = ReducerWorkflow.KEY_SEED;                         // seedKey (combine, then dropped)
    private static final String REPLACE = ReducerWorkflow.KEY_REPLACE;                   // replaceKey (local_only)
    private static final Object REPLACE_VAL = ReducerWorkflow.VALUE_REPLACE;             // "replaced"
    private static final String COMBINE = ReducerWorkflow.KEY_COMBINE;                   // combineKey (combine)
    private static final Object COMBINE_VAL = ReducerWorkflow.VALUE_COMBINE;             // "present"
    private static final String GLOBAL_ONLY = ReducerWorkflow.KEY_GLOBAL_ONLY;           // globalOnlyKey (discarded)
    private static final String PARAM = ReducerWorkflow.KEY_PARAM_SAW_COMBINE;           // paramSawCombineKey (combine)
    private static final String INTERPLAY = ReducerWorkflow.KEY_INTERPLAY;               // interplayKey (global_only discard)
    private static final String LWW = ReducerWorkflow.KEY_LWW;                           // lastWriterKey (combine, last wins)
    private static final Object LWW_FIRST = ReducerWorkflow.VALUE_LWW_FIRST;             // "first"
    private static final Object LWW_SECOND = ReducerWorkflow.VALUE_LWW_SECOND;           // "second"
    private static final String NULL_KEY = ReducerWorkflow.KEY_NULL;                     // nullValueKey (combine, null val)
    private static final String NULL_SIBLING = ReducerWorkflow.KEY_NULL_SIBLING;         // nullSiblingKey (combine)
    private static final Object NULL_SIBLING_VAL = ReducerWorkflow.VALUE_NULL_SIBLING;   // "sibling"

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void everyReducerProducesItsDocumentedMerge_stableAcrossReplay() {
        PayloadReducerSemanticsScenario.Outcome outcome = PayloadReducerSemanticsScenario.run(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("ReducerWorkflow must reach a terminal (COMPLETED) workflow status")
                .isTrue();
        Map<String, Object> payload = outcome.payloadBeforeCrash();
        // combine merged: the combine key is present with its value.
        assertThat(payload)
                .as("PayloadReducerSemantics: a combine result is merged into the payload")
                .containsEntry(COMBINE, COMBINE_VAL);
        // global_only discarded: the global_only result key is absent.
        assertThat(payload)
                .as("PayloadReducerSemantics: a global_only result is discarded (its key does not appear)")
                .doesNotContainKey(GLOBAL_ONLY);
        // local_only replace: the replacement key is present, the prior seed key dropped.
        assertThat(payload)
                .as("PayloadReducerSemantics: a local_only replace overwrites the whole payload (prior key dropped)")
                .containsEntry(REPLACE, REPLACE_VAL)
                .doesNotContainKey(SEED);
        // parameter-side combine view: the action saw the combined global payload.
        assertThat(payload)
                .as("PayloadReducerSemantics: a parameterPayloadReducer(combine) step sees the global payload")
                .containsEntry(PARAM, true);
        // edge (b) parameter-view vs result-write are independent knobs: the interplay step SAW the combined input but
        // its global_only result write was discarded — its key is absent.
        assertThat(payload)
                .as("PayloadReducerSemantics edge (b): a combine parameter view + global_only result write discards the result")
                .doesNotContainKey(INTERPLAY);
        // edge (c) last-writer-wins on the same key: the later same-key combine write wins.
        assertThat(payload)
                .as("PayloadReducerSemantics edge (c): the later same-key combine write wins")
                .containsEntry(LWW, LWW_SECOND);
        // Stable across replay: a crash + replay rebuilds the IDENTICAL payload — every reducer applied identically.
        assertThat(outcome.payloadAfterCrash())
                .as("PayloadReducerSemantics: replaying the same history applies every reducer identically")
                .isEqualTo(outcome.payloadBeforeCrash());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void nullValueUnderCombine_wedgesTheInstance_candidateFindingF6() {
        // Edge (a) — CANDIDATE FINDING F-6 (POC-TLA-DST.adoc, NOT patched per the test/docs-only rule): a combine step
        // whose result map carries a null value. The combine reducer (new HashMap<>(global); putAll(local)) ACCEPTS the
        // null and merges it, but the very next read of the workflow payload — EventSourcedWorkflowState.payload() doing
        // Map.copyOf(payload) (runtime ~line 176) — throws NullPointerException (Map.copyOf rejects null values). The NPE
        // surfaces at workflow completion and lands in handleWorkflowException's `default` branch, which deliberately does
        // NOT record a terminal status — so the instance is left NON-TERMINAL / stuck (a liveness stall), the NPE merely
        // logged. This test pins (characterizes) that observed behaviour; it does not assert a clean fold.
        PayloadReducerSemanticsScenario.NullEdgeOutcome outcome = PayloadReducerSemanticsScenario.runNullEdge(0L, "N");

        // The null DID reach the reducer (the combine step committed) — the edge is genuinely exercised.
        assertThat(outcome.nullCombineCommitted())
                .as("F-6: the null-value combine step's COMPLETED event committed (the null reached the reducer)")
                .isTrue();
        // OBSERVED: the instance does NOT reach a terminal status — the completion-path NPE wedged it.
        assertThat(outcome.reachedTerminal())
                .as("F-6: a null combine value wedges the instance — it does NOT reach a terminal status")
                .isFalse();
        assertThat(outcome.workflowCompleted())
                .as("F-6: no <workflow>:COMPLETED is recorded — the completion path threw before recording terminal")
                .isFalse();
        // OBSERVED: reading the engine's reconstructed payload itself throws the Map.copyOf null-value NPE.
        assertThat(outcome.payloadReadThrew())
                .as("F-6: reading the engine's reconstructed payload throws (Map.copyOf rejects the null value)")
                .isTrue();
        assertThat(outcome.payloadReadError())
                .as("F-6: the payload read throws a NullPointerException")
                .isEqualTo("NullPointerException");
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void throwingModifierLambda_failsTheWorkflowOnce_andARestartDoesNotReDriveIt() {
        // A plain RuntimeException thrown by a modifyPayload modifier lambda between primitives is a defect in the
        // body, not a recoverable exception: the engine ends the workflow FAILED instead of leaving it non-terminal.
        PayloadReducerSemanticsScenario.ThrowingModifierOutcome outcome =
                PayloadReducerSemanticsScenario.runThrowingModifierEdge(0L, "T");

        // The throw genuinely reached the user lambda on the workflow thread.
        assertThat(outcome.modifierRanBeforeCrash()).as("the modifyPayload modifier lambda ran").isTrue();
        // The modifier threw before PayloadDelegate publishes its COMPLETED, so no step record commits.
        assertThat(outcome.committedStepRecords()).as("the throw is between primitives").isZero();
        assertThat(outcome.terminalBeforeCrash())
                .as("the unhandled RuntimeException ends the workflow FAILED")
                .isEqualTo(WorkflowStatus.FAILED);
        // Nothing after the terminal: a crash + recover neither re-drives nor re-publishes.
        assertThat(outcome.terminalRecordsAfterRecover()).as("exactly one terminal record").isEqualTo(1);
        assertThat(outcome.modifierRunsAfterRecover()).as("the modifier is not re-run").isEqualTo(1);
        assertThat(outcome.liveAfterRecover()).as("the failed instance is not restored").isFalse();
    }

    @Test
    void assertPayloadReducerSemantics_passesWhenPayloadMatchesDocumentedFold() {
        // The full workflow shape: combine seed, local_only replace dropping it, combine, global_only (discarded),
        // parameter-view combine, interplay (global_only discard), last-writer-wins pair. The engine's payload reflects
        // each reducer's documented outcome (the discarded keys absent, the last writer winning lastWriterKey).
        List<EventMessage> log = workflowShape("reducer-r0");
        Map<String, Map<String, Object>> actual = Map.of(
                "reducer-r0", Map.of(REPLACE, REPLACE_VAL, COMBINE, COMBINE_VAL, PARAM, true, LWW, LWW_SECOND));

        assertThatCode(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .as("a payload matching the documented reducer fold is sound")
                .doesNotThrowAnyException();
    }

    @Test
    void assertPayloadReducerSemantics_throwsWhenInterplayGlobalOnlyResultWronglyAppears() {
        // Edge (b) break: the interplay step has a combine PARAMETER view but a global_only RESULT write — its result is
        // DISCARDED by the documented fold. If the engine's payload WRONGLY carries the interplay key, the two reducers
        // were not treated as independent knobs (a combined input wrongly implied a combined write-back).
        List<EventMessage> log = List.of(
                globalOnly("reducer-r0", ReducerWorkflow.STEP_INTERPLAY, Map.of(INTERPLAY, true)));
        Map<String, Map<String, Object>> actual = Map.of("reducer-r0", Map.of(INTERPLAY, true));

        assertThatThrownBy(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("PayloadReducerSemantics")
                .hasMessageContaining(INTERPLAY);
    }

    @Test
    void assertPayloadReducerSemantics_throwsWhenEarlierSameKeyWriterWronglyWins() {
        // Edge (c) break: two combine steps write lastWriterKey; the LATER one ("second") must win per the documented
        // fold. If the engine's payload carries the FIRST writer's value ("first"), last-writer-wins was violated.
        List<EventMessage> log = List.of(
                combine("reducer-r0", ReducerWorkflow.STEP_LWW_FIRST, Map.of(LWW, LWW_FIRST)),
                combine("reducer-r0", ReducerWorkflow.STEP_LWW_SECOND, Map.of(LWW, LWW_SECOND)));
        Map<String, Map<String, Object>> actual = Map.of("reducer-r0", Map.of(LWW, LWW_FIRST)); // earlier writer wrongly wins

        assertThatThrownBy(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("PayloadReducerSemantics")
                .hasMessageContaining(LWW);
    }

    @Test
    void assertPayloadReducerSemantics_combineKeepsAGlobalKeyAStepOmits() {
        // Edge (d) missing keys under combine: a combine step's result map OMITS a key the running payload already holds
        // (the seed combine writes seedKey; the later combine writes only combineKey, omitting seedKey). Per the
        // documented fold (putAll merges only the present keys) the omitted key KEEPS its prior value — both survive.
        List<EventMessage> log = List.of(
                combine("reducer-r0", ReducerWorkflow.STEP_SEED, Map.of(SEED, true)),
                combine("reducer-r0", ReducerWorkflow.STEP_COMBINE, Map.of(COMBINE, COMBINE_VAL)));
        Map<String, Map<String, Object>> actual = Map.of(
                "reducer-r0", Map.of(SEED, true, COMBINE, COMBINE_VAL)); // the omitted seedKey survives the later combine

        assertThatCode(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .as("a combine that omits a key keeps that key's prior (global) value")
                .doesNotThrowAnyException();
    }

    @Test
    void assertPayloadReducerSemantics_nullValueUnderCombineFoldsToKeyPresentWithNull() {
        // Edge (a) at the HARNESS-FOLD level (independent of the engine's F-6 defect — see
        // nullValueUnderCombine_wedgesTheInstance_candidateFindingF6): the harness's documented fold
        // (new HashMap<>(global); putAll(local)) inserts the null, so (A) carries the key with value null. A
        // hypothetical engine payload (B) matching (key present, value null) passes — pinning that the harness models the
        // combine-null semantics, so a future engine that handled nulls cleanly would be checked correctly.
        List<EventMessage> log = List.of(
                combineWithNull("reducer-r0", ReducerWorkflow.STEP_NULL_COMBINE));
        var actualPayload = new java.util.HashMap<String, Object>();
        actualPayload.put(NULL_KEY, null);                 // key PRESENT with value null — the documented combine fold.
        actualPayload.put(NULL_SIBLING, NULL_SIBLING_VAL);
        Map<String, Map<String, Object>> actual = Map.of("reducer-r0", actualPayload);

        assertThatCode(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .as("combine keeps the key with a null value (putAll inserts null) — matches the engine")
                .doesNotThrowAnyException();
    }

    @Test
    void assertPayloadReducerSemantics_throwsWhenCombineNullValueIsDropped() {
        // Edge (a) HARNESS break: the harness fold keeps nullValueKey with value null, but a payload (B) DROPPED it.
        // (A) has the key, (B) lacks it — flagged as a divergence mentioning PayloadReducerSemantics + the key. This pins
        // that the harness would surface a null-dropping divergence (a different anomaly from the F-6 wedge above).
        List<EventMessage> log = List.of(
                combineWithNull("reducer-r0", ReducerWorkflow.STEP_NULL_COMBINE));
        Map<String, Map<String, Object>> actual = Map.of(
                "reducer-r0", Map.of(NULL_SIBLING, NULL_SIBLING_VAL)); // nullValueKey wrongly dropped

        assertThatThrownBy(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("PayloadReducerSemantics")
                .hasMessageContaining(NULL_KEY);
    }

    @Test
    void assertPayloadReducerSemantics_isContentBased_toleratesReorderedGlobalAppend() {
        // F-2 pin: the per-instance documented fold depends on the per-instance step SEQUENCE (which IS deterministic),
        // never on the global-append order. Here r0's two same-key combine writes are interleaved with an UNRELATED
        // r1 event in the global log; r0's later writer still wins. Reordering the global append (the r1 event before vs
        // after r0's) does not change r0's per-instance fold, so the same engine payload passes either ordering.
        List<EventMessage> ordering1 = List.of(
                combine("reducer-r0", ReducerWorkflow.STEP_LWW_FIRST, Map.of(LWW, LWW_FIRST)),
                combine("reducer-r1", ReducerWorkflow.STEP_COMBINE, Map.of(COMBINE, COMBINE_VAL)),
                combine("reducer-r0", ReducerWorkflow.STEP_LWW_SECOND, Map.of(LWW, LWW_SECOND)));
        List<EventMessage> ordering2 = List.of(
                combine("reducer-r1", ReducerWorkflow.STEP_COMBINE, Map.of(COMBINE, COMBINE_VAL)),
                combine("reducer-r0", ReducerWorkflow.STEP_LWW_FIRST, Map.of(LWW, LWW_FIRST)),
                combine("reducer-r0", ReducerWorkflow.STEP_LWW_SECOND, Map.of(LWW, LWW_SECOND)));
        Map<String, Map<String, Object>> actual = Map.of(
                "reducer-r0", Map.of(LWW, LWW_SECOND),
                "reducer-r1", Map.of(COMBINE, COMBINE_VAL));

        assertThatCode(() -> Invariants.assertPayloadReducerSemantics(ordering1, PREFIX, actual))
                .as("content-based: per-instance fold under one global ordering passes")
                .doesNotThrowAnyException();
        assertThatCode(() -> Invariants.assertPayloadReducerSemantics(ordering2, PREFIX, actual))
                .as("content-based: the SAME per-instance content under a reordered global append still passes (F-2-robust)")
                .doesNotThrowAnyException();
    }

    @Test
    void assertPayloadReducerSemantics_throwsWhenGlobalOnlyResultWronglyAppears() {
        // The break: a global_only step's result is discarded by the documented fold, but the engine's payload WRONGLY
        // carries its key — the engine applied global_only more permissively than documented.
        List<EventMessage> log = List.of(
                combine("reducer-r0", ReducerWorkflow.STEP_COMBINE, Map.of(COMBINE, COMBINE_VAL)),
                globalOnly("reducer-r0", ReducerWorkflow.STEP_GLOBAL_ONLY, Map.of(GLOBAL_ONLY, "discarded")));
        // globalOnlyKey WRONGLY present.
        Map<String, Map<String, Object>> actual = Map.of(
                "reducer-r0", Map.of(COMBINE, COMBINE_VAL, GLOBAL_ONLY, "discarded"));

        assertThatThrownBy(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("PayloadReducerSemantics")
                .hasMessageContaining("reducer-r0")
                .hasMessageContaining(GLOBAL_ONLY);
    }

    @Test
    void assertPayloadReducerSemantics_throwsWhenCombineKeyIsMissing() {
        // The break: a combine step's key must be merged, but the engine's payload is MISSING it — combine did not apply
        // its documented merge.
        List<EventMessage> log = List.of(
                combine("reducer-r0", ReducerWorkflow.STEP_COMBINE, Map.of(COMBINE, COMBINE_VAL)));
        Map<String, Map<String, Object>> actual = Map.of("reducer-r0", Map.of());

        assertThatThrownBy(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("PayloadReducerSemantics")
                .hasMessageContaining("reducer-r0")
                .hasMessageContaining(COMBINE);
    }

    @Test
    void assertPayloadReducerSemantics_throwsWhenLocalOnlyReplaceDidNotDropPriorKey() {
        // The break: a local_only replace overwrites the WHOLE payload, so the prior seed key must be dropped by the
        // documented fold — but the engine's payload still carries it (the replace failed to drop a prior key).
        List<EventMessage> log = List.of(
                combine("reducer-r0", ReducerWorkflow.STEP_SEED, Map.of(SEED, true)),
                replace("reducer-r0", ReducerWorkflow.STEP_LOCAL_ONLY_REPLACE, Map.of(REPLACE, REPLACE_VAL)));
        // seedKey WRONGLY still present after the local_only replace.
        Map<String, Map<String, Object>> actual = Map.of(
                "reducer-r0", Map.of(REPLACE, REPLACE_VAL, SEED, true));

        assertThatThrownBy(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("PayloadReducerSemantics")
                .hasMessageContaining("reducer-r0")
                .hasMessageContaining(SEED);
    }

    @Test
    void assertPayloadReducerSemantics_throwsWhenAReducerAppliedADifferentValue() {
        // The break: combine merged combineKey, but the engine's payload carries a DIFFERENT value for it — a reducer
        // applied a value the documented fold did not produce.
        List<EventMessage> log = List.of(
                combine("reducer-r0", ReducerWorkflow.STEP_COMBINE, Map.of(COMBINE, COMBINE_VAL)));
        Map<String, Map<String, Object>> actual = Map.of("reducer-r0", Map.of(COMBINE, "tampered"));

        assertThatThrownBy(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("PayloadReducerSemantics")
                .hasMessageContaining(COMBINE);
    }

    @Test
    void assertPayloadReducerSemantics_toleratesCrossInstancePayloads() {
        // Two INDEPENDENT reducer instances interleaved in the global log, each with its own payload matching its own
        // documented fold. INV-19 is per-instance, so r1's payload is not "wrong for r0" — must pass.
        List<EventMessage> log = List.of(
                combine("reducer-r0", ReducerWorkflow.STEP_COMBINE, Map.of(COMBINE, COMBINE_VAL)),
                combine("reducer-r1", ReducerWorkflow.STEP_COMBINE, Map.of(COMBINE, COMBINE_VAL)));
        Map<String, Map<String, Object>> actual = Map.of(
                "reducer-r0", Map.of(COMBINE, COMBINE_VAL),
                "reducer-r1", Map.of(COMBINE, COMBINE_VAL));

        assertThatCode(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .as("independent instances' payloads are checked per-workflowId")
                .doesNotThrowAnyException();
    }

    @Test
    void assertPayloadReducerSemantics_skipsInstanceWithNoReconstructedPayloadYet() {
        // The instance has committed a contribution but the engine has not reconstructed a payload for it yet (absent
        // from the actual map) — nothing to cross-check, so it must be skipped (no false positive mid-run).
        List<EventMessage> log = List.of(
                combine("reducer-r0", ReducerWorkflow.STEP_COMBINE, Map.of(COMBINE, COMBINE_VAL)));

        assertThatCode(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, Map.of()))
                .as("an instance with no reconstructed payload yet is skipped")
                .doesNotThrowAnyException();
    }

    @Test
    void assertPayloadReducerSemantics_skipsOutOfScopePrefix() {
        // A different-prefix instance is not constrained by INV-19 — even a (hypothetical) mismatching payload on it must
        // be skipped, not flagged (INV-19 is scoped to the reducer workflow's id prefix).
        List<EventMessage> log = List.of(
                combine("payload-wf0", ReducerWorkflow.STEP_COMBINE, Map.of(COMBINE, COMBINE_VAL)));
        Map<String, Map<String, Object>> actual = Map.of("payload-wf0", Map.of()); // would mismatch if checked

        assertThatCode(() -> Invariants.assertPayloadReducerSemantics(log, PREFIX, actual))
                .as("non-reducer (payload-) instances are out of INV-19's scope and are skipped")
                .doesNotThrowAnyException();
    }

    /**
     * The full {@link ReducerWorkflow} committed shape: combine seed, local_only replace (drops the seed), combine,
     * global_only (discarded), parameter-view combine, interplay (combine input but global_only write → discarded), and
     * the last-writer-wins same-key combine pair. The documented fold of this is
     * {@code {replaceKey, combineKey, paramSawCombineKey, lastWriterKey=second}}.
     */
    private static List<EventMessage> workflowShape(String workflowId) {
        return List.of(
                combine(workflowId, ReducerWorkflow.STEP_SEED, Map.of(SEED, true)),
                replace(workflowId, ReducerWorkflow.STEP_LOCAL_ONLY_REPLACE, Map.of(REPLACE, REPLACE_VAL)),
                combine(workflowId, ReducerWorkflow.STEP_COMBINE, Map.of(COMBINE, COMBINE_VAL)),
                globalOnly(workflowId, ReducerWorkflow.STEP_GLOBAL_ONLY, Map.of(GLOBAL_ONLY, "discarded")),
                combine(workflowId, ReducerWorkflow.STEP_PARAM_VIEW, Map.of(PARAM, true)),
                globalOnly(workflowId, ReducerWorkflow.STEP_INTERPLAY, Map.of(INTERPLAY, true)),
                combine(workflowId, ReducerWorkflow.STEP_LWW_FIRST, Map.of(LWW, LWW_FIRST)),
                combine(workflowId, ReducerWorkflow.STEP_LWW_SECOND, Map.of(LWW, LWW_SECOND)));
    }

    private static EventMessage combine(String workflowId, String stepName, Map<String, Object> result) {
        return payloadStep(workflowId, stepName, result, CombineGlobalAndLocalPayloadReducer.NAME);
    }

    /**
     * A combine COMPLETED step whose result map carries a {@code null} value under {@link #NULL_KEY} (edge (a)),
     * alongside a non-null sibling. Built with a {@link java.util.HashMap} since {@link Map#of} rejects null values.
     */
    private static EventMessage combineWithNull(String workflowId, String stepName) {
        var result = new java.util.HashMap<String, Object>();
        result.put(NULL_KEY, null);
        result.put(NULL_SIBLING, NULL_SIBLING_VAL);
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
