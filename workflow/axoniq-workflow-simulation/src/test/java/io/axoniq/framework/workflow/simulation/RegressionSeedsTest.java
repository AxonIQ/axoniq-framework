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
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.DstSimulation;
import io.axoniq.framework.workflow.simulation.harness.SimulationConfig;
import io.axoniq.framework.workflow.simulation.harness.SimulationResult;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Permanent regression guard for the Phase-6 fuzz campaign and for the one issue it surfaced.
 * <p>
 * The large fuzz campaign (≥1000 seeds across the full fault set, incl. write-then-vanish) found <strong>one</strong>
 * issue: a <em>harness</em> bug, not an engine bug. {@code Invariants.assertCommittedHistorySurvivesCrash} (INV-3,
 * called by {@code WorkerCrashFault} / {@code RestartFault} after a recovery) compared the two committed logs by
 * <em>absolute global index</em>. With several concurrent {@code OrderWorkflow} instances, the cross-instance
 * interleaving of the single global log is non-deterministic across a crash+replay (it is the F-2 surface the harness
 * deliberately factors out everywhere else — {@code assertDeterministicReplay}, the intra-run prefix check, the
 * fingerprint all group by {@code workflowId}). Two independent instances swapping their global position therefore
 * tripped the global-index comparison even though every instance's own committed history was intact. The fix made
 * {@code assertCommittedHistorySurvivesCrash} compare <strong>per-{@code workflowId} subsequences</strong>, matching
 * INV-3's real meaning and the harness's documented per-instance contract. No production engine code was changed.
 * <p>
 * Signature of the finding (recorded so it can never silently regress):
 * <ul>
 *   <li>timing-dependent, <em>not</em> seed-determined — the same seed passed on some passes and tripped on others in
 *       the same JVM (first observed on seeds {@code 74} and {@code 104} under repeated passes);</li>
 *   <li>every observed message was two <em>distinct</em> instances swapping a global index
 *       (e.g. {@code before=order-wf0:reserveInventory:STARTED after=order-wf4:reserveInventory:STARTED}), never a
 *       within-instance reorder and never a lost event.</li>
 * </ul>
 * This class locks three guards: (1) a fixed regression seed-set — including the two seeds that surfaced the bug — run
 * end-to-end through the full fuzz config, asserting every invariant holds; (2) a deterministic pin that a
 * cross-instance interleave is tolerated; (3) the complementary pin that a genuine per-instance loss is still detected,
 * so the fix did not weaken the durability invariant.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class RegressionSeedsTest {

    /**
     * Fixed regression seed-set explored under the FULL fuzz config (every fault, incl. write-then-vanish), asserted to
     * keep all invariants green (INV-1..6 plus INV-7 {@code TerminalIsFinal}, INV-8 {@code RetryBound}, INV-10
     * {@code OneInstancePerStart}, INV-11 {@code VersionRoutingSound}, INV-12 {@code MigrateVersionContract}, INV-13
     * {@code NoLostPayloadWrites}, INV-14 {@code CombinatorConsistency}, INV-15 {@code EventCorrelationExact}, INV-19
     * {@code PayloadReducerSemantics}, INV-20 {@code VersioningEdges} and INV-22 {@code EventNameCustomizationSound}).
     * Includes seeds {@code 74} and {@code 104},
     * which surfaced the {@code CommittedHistorySurvivesCrash} global-index harness bug under repeated passes, and seed
     * {@code 370}, which surfaced a second (same-class) harness recording artifact — the parallel committed-log lists
     * diverging in order under the {@code CombinatorWorkflow}'s rapid concurrent commits, fixed by collapsing
     * {@code ControllableEventStorageEngine} to a single committed-event source (see POC-TLA-DST.adoc F-2) — plus a
     * spread of others so the guard covers more than just the known triggers. Kept small so it runs in the per-PR
     * build (it is <em>not</em> {@code @Tag("fuzz")}); the nightly {@link DstFuzzTest} sweeps thousands.
     */
    static final long[] REGRESSION_SEEDS =
            {0L, 1L, 5L, 7L, 11L, 12L, 13L, 17L, 18L, 33L, 42L, 60L, 66L, 74L, 77L, 88L, 99L, 104L, 144L, 200L, 222L,
                    250L, 252L, 300L, 321L, 333L, 370L, 401L, 444L, 499L, 512L, 555L, 666L, 800L, 911L};

    @ParameterizedTest(name = "fuzz seed {0}: all invariants hold across the full-fault run (regression guard)")
    @ValueSource(longs = {0L, 1L, 5L, 7L, 11L, 12L, 13L, 17L, 18L, 33L, 42L, 60L, 66L, 74L, 77L, 88L, 99L, 104L, 144L,
            200L, 222L, 250L, 252L, 300L, 321L, 333L, 370L, 401L, 444L, 499L, 512L, 555L, 666L, 800L, 911L})
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void fixedRegressionSeedSet_allInvariantsHold(long seed) {
        // SimulationConfig.fuzz = 5 instances, 60 steps, p(fault)=0.7, ALL_FAULTS. A genuine break throws an enriched
        // InvariantViolation (seed + reproduce command + fault trace + committed log); reaching the assertions means
        // every instance terminated and no invariant broke for this seed (incl. INV-7 TerminalIsFinal, INV-8 RetryBound,
        // INV-10 OneInstancePerStart, INV-11 VersionRoutingSound, INV-12 MigrateVersionContract and INV-13
        // NoLostPayloadWrites, all asserted after every step in DstSimulation across the crash/recover and
        // duplicate/redeliver faults — INV-8 against OrderWorkflow's genuinely-retrying shipOrder step; INV-11 against a
        // two-version VersionedOrderWorkflow; INV-12 against a MigratingOrderWorkflow whose body calls ctx.migrateVersion;
        // INV-13 against a PayloadOrderWorkflow whose every step writes a distinct payload key). Seeds 17 and 200 were
        // added with INV-7; 5 and 300 with INV-8; 11 and 88 with INV-10 (the MESSAGE_REORDER duplicate mode redelivers an
        // OrderPlacedEvent — INV-10's live-dedup must keep it from opening a second concurrent instance); 33 and 321
        // with INV-11 (the VersionedOrderWorkflow is registered at two versions and crashed/restarted/reordered; a fresh
        // start must spawn at the highest version and every event of the instance must carry exactly that one version);
        // 12 and 144 with INV-12 (the MigratingOrderWorkflow's in-body ctx.migrateVersion marker is crashed/restarted/
        // reordered; the recorded migration version must stay written-once, monotonic, and unchanged across replay);
        // 60 and 222 with INV-13 (the PayloadOrderWorkflow writes a distinct payload key per step — three execute+combine
        // merges then one modifyPayload replace — crashed/restarted/reordered; the final committed payload, rebuilt from
        // the log as the engine evolves it, must reflect every committed contribution with no lost write); 66 and 333
        // with INV-14 (the CombinatorWorkflow runs three parallel branches — A=yes, B=no, C=no — folded through
        // anyMatch/allMatch/noneMatch, crashed/restarted/reordered; each combinator's recorded decision, re-derived from
        // its committed branch outcomes, must stay consistent and unchanged across replay); 77 and 444 with INV-15 (the
        // two CorrelatedWaitWorkflow instances wait on distinct keys A and B; each instance's matching CorrelatedSignalEvent
        // rides the MESSAGE_REORDER reorder/delay/DUPLICATE path crashed/restarted/reordered; every completed wait must
        // have matched on its OWN key — no cross-wakeup — and completed at most once even when the matching signal is
        // duplicated). Seeds 370 and 252 were added with the F-2 intra-instance append-order harness fixes: both
        // (timing-dependently) tripped an order-sensitive invariant on a tightly-committing new workflow — 370 tripped
        // INV-3 CommittedHistorySurvivesCrash on the combinator instance (the harness kept two parallel committed-log
        // lists whose append order could diverge under rapid concurrent commits; now a single committedTagged source),
        // and 252+18 tripped INV-7 TerminalIsFinal on tightly-committing instances (a step's own event globally appended
        // just after its workflow-completion event because the engine publishes durably-async — 252 a late COMPLETED on
        // the correlated-wait instance, 18 a late STARTED on the migrating instance whose COMPLETED was already committed;
        // assertTerminalIsFinal now flags only NEW work after terminal — a workflow-status event, or a step with no
        // pre-terminal event — and tolerates any late-appended event of a step that began before terminal). All were
        // recording/ordering artifacts, not engine/durability defects (events present, state correct). See POC-TLA-DST.adoc F-2.
        // Seeds 555 and 666 were added with INV-19 PayloadReducerSemantics (the ReducerWorkflow exercises all three
        // payload reducers — a combine seed, a local_only modifyPayload replace dropping the seed, a combine that must
        // appear, a global_only execute whose result must be discarded, and a parameterPayloadReducer(combine) step —
        // crashed/restarted/reordered; the engine's reconstructed payload must equal the documented reducer fold of the
        // committed log, content-based / F-2-robust, stable across replay).
        // Seeds 401 and 512 were added with INV-20 VersioningEdges (the VersioningEdgesWorkflow is registered at THREE
        // versions; a fresh start spawns at the highest and the body performs two forward ctx.migrateVersion bumps under
        // distinct changeIds then a rejected downgrade — crashed/restarted/reordered; no marker may be recorded for the
        // downgrade changeId, each changeId marker stays written-once and monotonic, and the spawn stays at the highest
        // of the three registered versions across replay).
        // Seeds 800 and 911 were added with INV-22 EventNameCustomizationSound (the CustomNamedWorkflow is registered
        // with a custom eventNameCustomizer — custom namespace + workflow base name + a stepCompleted("Done") override;
        // crashed/restarted/reordered, every committed step/status event must carry the EXPECTED customized wire name
        // and those names must stay identical across replay — a pure function of history, content-based / F-2-robust).
        SimulationResult result = new DstSimulation(SimulationConfig.fuzz(seed)).run();
        assertThat(result.committedLog())
                .as("fuzz seed %d must produce a non-empty committed log", seed)
                .isNotEmpty();
        assertThat(Invariants.workflowIdsIn(result.committedLog()))
                .as("fuzz seed %d drives 5 OrderWorkflow + 1 VersionedOrderWorkflow (INV-11) + 1 MigratingOrderWorkflow"
                            + " (INV-12) + 1 PayloadOrderWorkflow (INV-13) + 1 CombinatorWorkflow (INV-14) + 2"
                            + " CorrelatedWaitWorkflow (INV-15) + 1 ReducerWorkflow (INV-19) + 1 VersioningEdgesWorkflow"
                            + " (INV-20) + 1 CustomNamedWorkflow (INV-22) + 4 P-series production-realism singletons"
                            + " (saga[retry-comp] + subscription + rollingDeploy[v2] + counterLoop) instances", seed)
                .hasSize(18);
    }

    @Test
    void committedHistorySurvivesCrash_toleratesCrossInstanceInterleaving() {
        // The exact shape the harness bug tripped on: two INDEPENDENT instances (wf0, wf4) each commit one
        // reserveInventory:STARTED event. Across a crash+replay the single global log re-interleaves them — wf4's event
        // lands before wf0's. Each instance's OWN subsequence is intact, so INV-3 (a per-instance durability property)
        // must NOT flag this; a raw global-index comparison would (the bug).
        List<EventMessage> beforeCrash = List.of(
                started("order-wf0", "reserveInventory"),
                started("order-wf4", "reserveInventory"));
        List<EventMessage> afterRecovery = List.of(
                started("order-wf4", "reserveInventory"),
                started("order-wf0", "reserveInventory"));

        assertThatCode(() -> Invariants.assertCommittedHistorySurvivesCrash(beforeCrash, afterRecovery))
                .as("cross-instance interleaving (the F-2 surface) must not be flagged as a durability break")
                .doesNotThrowAnyException();
    }

    @Test
    void committedHistorySurvivesCrash_stillDetectsPerInstanceLoss() {
        // Complementary guard: the fix must not weaken the invariant. If an instance's OWN committed event is missing
        // after recovery, that is a genuine durability break and must still throw.
        List<EventMessage> beforeCrash = List.of(
                started("order-wf0", "reserveInventory"),
                completed("order-wf0", "reserveInventory"));
        List<EventMessage> afterRecovery = List.of(
                started("order-wf0", "reserveInventory")); // wf0's COMPLETED was lost across recovery

        assertThatThrownBy(() -> Invariants.assertCommittedHistorySurvivesCrash(beforeCrash, afterRecovery))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("CommittedHistorySurvivesCrash")
                .hasMessageContaining("order-wf0");
    }

    private static EventMessage started(String workflowId, String stepName) {
        return event(workflowId, stepName, StepStatus.STARTED);
    }

    private static EventMessage completed(String workflowId, String stepName) {
        return event(workflowId, stepName, StepStatus.COMPLETED);
    }

    private static EventMessage event(String workflowId, String stepName, StepStatus status) {
        Metadata metadata = MetadataUtils.create(workflowId, stepName, status);
        return new GenericEventMessage(new MessageType(stepName), Map.of(), metadata);
    }
}
