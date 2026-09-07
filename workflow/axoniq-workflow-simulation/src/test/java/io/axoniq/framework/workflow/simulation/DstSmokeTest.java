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

import io.axoniq.framework.workflow.simulation.faults.FaultKind;
import io.axoniq.framework.workflow.simulation.harness.DstSimulation;
import io.axoniq.framework.workflow.simulation.harness.SimulationConfig;
import io.axoniq.framework.workflow.simulation.harness.SimulationResult;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Per-PR smoke suite for the deterministic simulator. It runs a small fixed seed set, asserting the protocol
 * invariants hold after every step (INV-1..6 plus the DST-only INV-7..22; the loop throws an enriched
 * {@code InvariantViolation} on any genuine break), and proves same-seed reproducibility. The expected gaps F-0 and
 * F-1 are exercised as documented expected violations in {@link F0EffectDuplicationTest} and {@link F1SplitBrainTest},
 * so this suite stays green.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class DstSmokeTest {

    /**
     * The fixed smoke seed set. Small and fast (the per-PR build keeps this tight — under a minute); the nightly fuzz
     * covers thousands via {@code -Ddst.seeds}.
     */
    static final long[] SMOKE_SEEDS = {1L, 2L, 3L, 42L};

    @ParameterizedTest(name = "seed {0}: all invariants hold across the simulated run")
    @ValueSource(longs = {1L, 2L, 3L, 42L})
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void allInvariantsHold(long seed) {
        // The simulation asserts INV-1..6 plus the DST-only INV-7..22 after every step; any genuine break throws
        // (failing the test) with the seed + committed log printed. Reaching here means every instance terminated and
        // no invariant broke.
        SimulationResult result = new DstSimulation(SimulationConfig.smoke(seed)).run();
        assertThat(result.committedLog()).isNotEmpty();
        // 3 OrderWorkflow + the 1 VersionedOrderWorkflow (INV-11) + the 1 MigratingOrderWorkflow (INV-12) + the 1
        // PayloadOrderWorkflow (INV-13) + the 1 CombinatorWorkflow (INV-14) + the 2 CorrelatedWaitWorkflow (INV-15) + the
        // 1 ReducerWorkflow (INV-19) + the 1 VersioningEdgesWorkflow (INV-20) + the 1 CustomNamedWorkflow (INV-22)
        // instances the harness also drives, plus the 4 P-series production-realism singletons (saga[retry-comp] +
        // subscription + rollingDeploy[spawns at v2] + counterLoop) = 16.
        assertThat(Invariants.workflowIdsIn(result.committedLog())).hasSize(16);
    }

    @ParameterizedTest(name = "seed {0}: same seed twice yields identical per-instance committed log")
    @ValueSource(longs = {1L, 2L, 3L, 42L})
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void sameSeedIsReproducible(long seed) {
        SimulationResult first = new DstSimulation(SimulationConfig.smoke(seed)).run();
        SimulationResult second = new DstSimulation(SimulationConfig.smoke(seed)).run();

        // DeterministicReplay (INV-4), run granularity: the per-instance committed subsequences must be identical.
        Invariants.assertDeterministicReplay(first.committedLog(), second.committedLog());
        assertThat(second.logFingerprint()).isEqualTo(first.logFingerprint());
        assertThat(second.effectCounts()).isEqualTo(first.effectCounts());
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void smokeSeedSetSummary() {
        // A single consolidated run over the whole smoke set, summarizing what was checked for the build log.
        int totalWorkflows = 0;
        for (long seed : SMOKE_SEEDS) {
            SimulationResult result = new DstSimulation(SimulationConfig.smoke(seed)).run();
            totalWorkflows += Invariants.workflowIdsIn(result.committedLog()).size();
        }
        // 3 OrderWorkflow + 1 VersionedOrderWorkflow (INV-11) + 1 MigratingOrderWorkflow (INV-12) + 1 PayloadOrderWorkflow
        // (INV-13) + 1 CombinatorWorkflow (INV-14) + 2 CorrelatedWaitWorkflow (INV-15) + 1 ReducerWorkflow (INV-19) + 1
        // VersioningEdgesWorkflow (INV-20) + 1 CustomNamedWorkflow (INV-22) + 4 P-series production-realism singletons
        // (saga[retry-comp] + subscription + rollingDeploy[spawns at v2] + counterLoop) = 16 instances per seed.
        assertThat(totalWorkflows).isEqualTo(SMOKE_SEEDS.length * 16);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void wallClockDeadlineAbortsRatherThanHangs() {
        // Anti-hang guarantee: with an unreachably short wall-clock deadline the run aborts almost immediately with an
        // enriched diagnostic (machine name + seed + reproduce command + committed log) instead of blocking. This is
        // the property the user asked for — the harness is structurally incapable of hanging; the JUnit @Timeout here
        // sits far above the harness deadline and must never be what stops the run.
        var hangBait = new SimulationConfig(1L, 3, 24, 0.0, FaultKind.values(), Duration.ofMillis(1));
        assertThatThrownBy(() -> new DstSimulation(hangBait).run())
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("HARNESS ABORT")
                .hasMessageContaining("seed=1")
                .hasMessageContaining("reproduce:");
    }
}
