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
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CombinatorWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.MigratingOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.RetryingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CombinatorRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.MigrateRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryRequestedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 4 depth pins for {@code DUPLICATED_APPEND} (finding F-24) — the harder compositions beyond the
 * {@code DstWideningSmokeTest} simple-case pin: an at-least-once store duplicates a specific
 * NON-terminal ({@code RETRYING}), version-marker ({@code migrateVersion}) or combinator-decision record, then the
 * engine crashes and recovers over the duplicated durable log. Each probe separates STORE-level duplication
 * (visible in the raw log) from ENGINE misbehaviour (post-recovery appends, wrong state, extra attempts):
 * replay idempotence HOLDS in every composition (the engine appends nothing, applies the marker once via
 * {@code putIfAbsent}, and drives the vanished-terminal resume identically with or without the duplicate — see the
 * vanish-only control), and the record-counting invariants INV-8 / INV-12 hold over the duplicated log via the F-24
 * distinct-identifier hardening (a genuine engine re-publish mints a NEW identifier and is still caught).
 */
class DuplicatedAppendDepthProbeTest {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(DuplicatedAppendDepthProbeTest.class);

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void duplicatedRetryingRecord_thenCrashRecovery_replayIsIdempotent() {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(21L, EngineInstance.retryingWorkflow(effects))) {
            String workflowId = "retry-dupR";
            world.eventStore().armDuplicateCommitFor(RetryingWorkflow.STEP_FLAKY, StepStatus.RETRYING);
            world.engine().publish(new RetryRequestedEvent("dupR"));
            awaitWorkflowTerminal(world, workflowId);

            long storeDups = storeLevelDuplicates(world);
            assertThat(storeDups).as("the RETRYING record is duplicated in the durable log").isPositive();

            int effectsBefore = effects.count(workflowId, RetryingWorkflow.STEP_FLAKY);
            int before = world.committedLog().size();
            world.crashAndRecover();
            allowSettle(world, before);

            logger.info("[PROBE dupRETRYING] log=" + world.eventStore().renderCommittedLog());
            assertThat(world.committedLog().size())
                    .as("crash+recovery replay over the duplicated RETRYING record appends nothing")
                    .isEqualTo(before);
            assertThat(effects.count(workflowId, RetryingWorkflow.STEP_FLAKY))
                    .as("recovery re-runs no flaky attempts (terminal instance)")
                    .isEqualTo(effectsBefore);
            // INV-8 over the store-duplicated log: holds under distinct-identifier counting (the F-24 hardening).
            Invariants.assertRetryBound(
                    world.committedLog(), Map.of(RetryingWorkflow.STEP_FLAKY, RetryingWorkflow.FLAKY_MAX_RETRIES));
            Invariants.assertTerminalIsFinal(world.committedLog());
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void duplicatedRetryingRecord_withVanishedTerminal_recoveryContinuesWithinBound() {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(22L, EngineInstance.retryingWorkflow(effects))) {
            String workflowId = "retry-dupRV";
            // Compose: the first RETRYING commit lands TWICE durable; the step's terminal FAILED commit VANISHES
            // (process dies mid-write) — so recovery replays a NON-terminal instance whose attempt history is
            // inflated by the duplicate. Does the recovered engine stay within the retry bound?
            world.eventStore().armDuplicateCommitFor(RetryingWorkflow.STEP_FLAKY, StepStatus.RETRYING);
            world.eventStore().armVanishCommitFor(RetryingWorkflow.STEP_FLAKY, StepStatus.FAILED);
            world.engine().publish(new RetryRequestedEvent("dupRV"));
            Polling.awaitOrFail(Duration.ofSeconds(10), "flaky FAILED commit to vanish",
                                () -> !world.eventStore().isVanishArmed());

            long storeDups = storeLevelDuplicates(world);
            logger.info("[PROBE dupRETRYING+vanishFAILED] pre-crash log="
                                       + world.eventStore().renderCommittedLog() + " storeDups=" + storeDups);

            world.crashAndRecover();
            awaitWorkflowTerminal(world, workflowId);
            logger.info("[PROBE dupRETRYING+vanishFAILED] post-recovery log="
                                       + world.eventStore().renderCommittedLog()
                                       + " flakyEffects=" + effects.count(workflowId, RetryingWorkflow.STEP_FLAKY));

            // INV-8 over the store-duplicated log: holds under distinct-identifier counting (the F-24 hardening).
            Invariants.assertRetryBound(
                    world.committedLog(), Map.of(RetryingWorkflow.STEP_FLAKY, RetryingWorkflow.FLAKY_MAX_RETRIES));
            Invariants.assertTerminalIsFinal(world.committedLog());
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void vanishedTerminalOnly_control_recoveryContinuesWithinBound() {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(22L, EngineInstance.retryingWorkflow(effects))) {
            String workflowId = "retry-ctlV";
            // CONTROL for the dup+vanish composition: ONLY the vanished terminal, no duplicate — isolates what the
            // duplicated RETRYING record adds on top of plain vanished-terminal recovery.
            world.eventStore().armVanishCommitFor(RetryingWorkflow.STEP_FLAKY, StepStatus.FAILED);
            world.engine().publish(new RetryRequestedEvent("ctlV"));
            Polling.awaitOrFail(Duration.ofSeconds(10), "flaky FAILED commit to vanish",
                                () -> !world.eventStore().isVanishArmed());

            world.crashAndRecover();
            awaitWorkflowTerminal(world, workflowId);
            logger.info("[PROBE vanishFAILED control] post-recovery log="
                                       + world.eventStore().renderCommittedLog()
                                       + " flakyEffects=" + effects.count(workflowId, RetryingWorkflow.STEP_FLAKY));

            Invariants.assertRetryBound(
                    world.committedLog(), Map.of(RetryingWorkflow.STEP_FLAKY, RetryingWorkflow.FLAKY_MAX_RETRIES));
            Invariants.assertTerminalIsFinal(world.committedLog());
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void duplicatedMigrateVersionMarker_thenCrashRecovery_versionAppliedOnce() {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(23L, EngineInstance.migratingOrderWorkflow(effects))) {
            String workflowId = "vmig-dupM";
            world.eventStore().armDuplicateCommitFor(MigratingOrderWorkflow.CHANGE_ID, StepStatus.COMPLETED);
            world.engine().publish(new MigrateRequestedEvent("dupM"));
            awaitWorkflowTerminal(world, workflowId);

            long storeDups = storeLevelDuplicates(world);
            assertThat(storeDups).as("the migrateVersion marker is duplicated in the durable log").isPositive();

            int before = world.committedLog().size();
            world.crashAndRecover();
            allowSettle(world, before);

            logger.info("[PROBE dupMARKER] log=" + world.eventStore().renderCommittedLog());
            assertThat(world.committedLog().size())
                    .as("crash+recovery replay over the duplicated migrateVersion marker appends nothing")
                    .isEqualTo(before);
            // Fresh instance must have taken the migrated branch exactly once, and never the v1 branch.
            assertThat(effects.count(workflowId, MigratingOrderWorkflow.STEP_PROCESS_V2))
                    .as("migrated branch effect ran exactly once")
                    .isEqualTo(1);
            assertThat(effects.count(workflowId, MigratingOrderWorkflow.STEP_CHARGE_V1))
                    .as("v1 branch never ran")
                    .isZero();
            // INV-12 over the store-duplicated marker: holds under distinct-identifier counting (F-24).
            Invariants.assertMigrateVersionContract(world.committedLog(), "vmig-");
            Invariants.assertTerminalIsFinal(world.committedLog());
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void duplicatedCombinatorDecisionRecord_thenCrashRecovery_decisionStable() {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(24L, EngineInstance.combinatorWorkflow(effects))) {
            String workflowId = "comb-dupC";
            world.eventStore().armDuplicateCommitFor(CombinatorWorkflow.STEP_ANY_MATCHED, StepStatus.COMPLETED);
            world.engine().publish(new CombinatorRequestedEvent("dupC"));
            awaitWorkflowTerminal(world, workflowId);

            long storeDups = storeLevelDuplicates(world);
            assertThat(storeDups).as("the combinator decision record is duplicated in the durable log").isPositive();

            int before = world.committedLog().size();
            world.crashAndRecover();
            allowSettle(world, before);

            logger.info("[PROBE dupDECISION] log=" + world.eventStore().renderCommittedLog());
            assertThat(world.committedLog().size())
                    .as("crash+recovery replay over the duplicated decision record appends nothing")
                    .isEqualTo(before);
            // INV-14 was robust to the store duplicate from the start (it derives decisions, never counts records).
            Invariants.assertCombinatorConsistency(world.committedLog(), "comb-", Map.of());
            Invariants.assertTerminalIsFinal(world.committedLog());
        }
    }

    // ------------------------------------------------------------------------------------------------

    private static void awaitWorkflowTerminal(SimulationWorld world, String workflowId) {
        Polling.awaitOrFail(Duration.ofSeconds(15), "instance " + workflowId + " to reach a terminal workflow status",
                            () -> world.committedLog().stream().anyMatch(e ->
                                    workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                                            && MetadataUtils.getWorkflowStatus(e.metadata())
                                                            .map(s -> s.isTerminal()).orElse(false)));
    }

    /** Give any (wrong) post-recovery re-drive a moment to append before asserting nothing did. */
    private static void allowSettle(SimulationWorld world, int before) {
        try {
            Polling.awaitOrFail(Duration.ofSeconds(2), "any spurious post-recovery append",
                                () -> world.committedLog().size() != before);
        } catch (IllegalStateException expectedQuiet) {
            // Quiet is the healthy outcome; the caller asserts the size explicitly.
        }
    }

    private static long storeLevelDuplicates(SimulationWorld world) {
        var tagged = world.eventStore().committedTaggedEvents();
        return tagged.size() - tagged.stream().map(t -> t.event().identifier()).distinct().count();
    }
}
