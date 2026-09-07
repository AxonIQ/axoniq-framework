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

import io.axoniq.framework.workflow.simulation.faults.FaultKind;

import java.time.Duration;

/**
 * Knobs for one {@link DstSimulation} run. Defaults are tuned for the fast per-PR smoke; the fuzz suite uses the same
 * shape with more instances/steps.
 *
 * @param seed             the seed that fully determines the run (RNG + id generator).
 * @param workflowCount    how many {@code OrderWorkflow} instances to drive.
 * @param maxSteps         maximum simulation steps before forcing the horizon (second anti-hang guard; exceeding it
 *                         aborts the run with the enriched diagnostic, treated as a potential liveness finding).
 * @param faultProbability probability in [0,1] that a given step injects a (non-NONE) fault.
 * @param faultKinds       the faults the loop may draw from (order is fixed so the RNG selection is reproducible).
 * @param wallClockDeadline hard real-time deadline for the whole {@link DstSimulation#run()}; the seeded loop checks it
 *                          every iteration and every bounded wait clamps to what remains, so the harness is
 *                          structurally incapable of hanging — on expiry it aborts with the enriched diagnostic
 *                          (seed + committed log + last step) rather than blocking. This is the PRIMARY anti-hang
 *                          stop; the JUnit {@code @Timeout} on the tests sits comfortably above it as a last resort.
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public record SimulationConfig(
        long seed,
        int workflowCount,
        int maxSteps,
        double faultProbability,
        FaultKind[] faultKinds,
        Duration wallClockDeadline,
        boolean deterministicCarrier,
        @org.jspecify.annotations.Nullable Long interleavingSeed
) {

    /**
     * Compatibility constructor — the pre-Phase-2 shape: production body executor (no deterministic carrier).
     */
    public SimulationConfig(long seed,
                            int workflowCount,
                            int maxSteps,
                            double faultProbability,
                            FaultKind[] faultKinds,
                            Duration wallClockDeadline) {
        this(seed, workflowCount, maxSteps, faultProbability, faultKinds, wallClockDeadline, false, null);
    }

    /**
     * Returns this configuration with the Phase-2 deterministic single-carrier body executor enabled.
     *
     * @param interleavingSeed {@code null} for FIFO pick order; a seed for reproducible seeded interleaving fuzz.
     * @return a copy with the deterministic carrier on.
     */
        public SimulationConfig withDeterministicCarrier(@org.jspecify.annotations.Nullable Long interleavingSeed) {
        return new SimulationConfig(seed, workflowCount, maxSteps, faultProbability, faultKinds, wallClockDeadline,
                                    true, interleavingSeed);
    }

    /**
     * The faults whose committed-log outcome is a deterministic function of the seed (crash, restart, reorder,
     * clock-jump). The write-then-vanish fault is deliberately excluded from the reproducible loop because its
     * outcome races body execution against the crash; it is reproduced separately by the deterministic
     * {@link io.axoniq.framework.workflow.simulation.scenarios.WriteThenVanishScenario}.
     */
    public static final FaultKind[] REPRODUCIBLE_FAULTS = {
            FaultKind.WORKER_CRASH, FaultKind.MESSAGE_REORDER, FaultKind.RESTART, FaultKind.CLOCK_JUMP};

    /**
     * Every fault including the racy write-then-vanish — used by a non-reproducibility fuzz that maximizes invariant
     * coverage (it asserts the invariants every step but does not claim bit-for-bit seed equality).
     */
    public static final FaultKind[] ALL_FAULTS = {
            FaultKind.WORKER_CRASH, FaultKind.MESSAGE_REORDER, FaultKind.RESTART, FaultKind.CLOCK_JUMP,
            FaultKind.WRITE_THEN_VANISH};

    /**
     * The chaos fault set for the heaviest campaign ({@link #chaos(long)}): every existing fault (including the racy
     * write-then-vanish) <em>plus</em> the five chaos faults — duplicate-completed (re-injects a committed terminal,
     * stressing INV-2 dedup), event-store latency jitter (extra settle nudges + aggressive batch shuffle, stressing
     * INV-4/ordering), flapping restart (rapid crash+recover cycles, stressing INV-3/INV-5), clock skew (varied-
     * magnitude forward jumps, stressing INV-5/timers) and partial batch (deliver only part of the batch, stressing
     * INV-2/ordering). It does <em>not</em> claim bit-for-bit seed equality (it asserts the invariants every step, the
     * {@code ALL_FAULTS} precedent).
     * <p>
     * Deliberately <strong>excludes</strong> any two-engine-over-shared-store segment-claim contention: that would
     * re-surface the known split-brain gap F-1 (it has no durable cross-node lease), turning the general chaos campaign
     * red over a <em>documented</em> gap. F-1 stays covered by its dedicated expected-violation tests
     * ({@code F1SplitBrainTest} / {@code F1RecordDuplicationTest} / {@code SplitBrainScenario}), per Recipe E (a).
     */
    public static final FaultKind[] CHAOS_FAULTS = {
            FaultKind.WORKER_CRASH, FaultKind.MESSAGE_REORDER, FaultKind.RESTART, FaultKind.CLOCK_JUMP,
            FaultKind.WRITE_THEN_VANISH, FaultKind.DUPLICATE_COMPLETED, FaultKind.EVENT_STORE_LATENCY_JITTER,
            FaultKind.FLAPPING_RESTART, FaultKind.CLOCK_SKEW, FaultKind.PARTIAL_BATCH,
            FaultKind.DUPLICATED_APPEND};

    /**
     * Returns a small, fast configuration for the per-PR smoke (a few instances, a handful of steps), using only the
     * reproducible fault set so the same seed yields the identical committed log.
     *
     * @param seed the seed.
     * @return a smoke-sized configuration.
     */
    public static SimulationConfig smoke(long seed) {
        // 60s deadline: a healthy smoke seed settles in a couple of seconds, so this only ever fires on a genuine
        // hang/liveness regression; the JUnit @Timeout backstops sit above it (see DstSmokeTest).
        // The smoke world drives 16 instances (3 OrderWorkflow + 13 singletons: versioned + migrating + payload +
        // combinator + 2 correlated + reducer + versioningEdges[INV-20] + customNamed[INV-22] + the four P-series
        // production-realism singletons: saga[retry-comp, happy] + subscription + rollingDeploy[v1+v2, spawns at v2] +
        // counterLoop). maxSteps bumped 24->60->80 as the singleton set grew: it is the SECONDARY anti-hang guard
        // (the wall-clock deadline is primary) and a stale cap becomes a load-sensitive spurious HARNESS-ABORT.
        // A healthy seed still finishes well under the cap; a genuine spin/stall is still caught by the deadline.
        return new SimulationConfig(seed, 3, 80, 0.5, REPRODUCIBLE_FAULTS, Duration.ofSeconds(60));
    }

    /**
     * Returns a heavier configuration for the nightly fuzz (more instances, more steps, more faults), using the full
     * fault set including write-then-vanish for maximum invariant coverage.
     *
     * @param seed the seed.
     * @return a fuzz-sized configuration.
     */
    public static SimulationConfig fuzz(long seed) {
        // A heavier run (more instances/steps/faults) gets a larger deadline, but still bounded so a single bad seed in
        // the nightly sweep aborts fast with its diagnostic instead of stalling the whole job. Drives 18 instances
        // (5 OrderWorkflow + the 13 singletons incl. the four P-series production-realism workloads). maxSteps bumped
        // 60->120->160 as the singleton set grew (secondary anti-hang cap; a stale cap becomes a load-sensitive
        // spurious HARNESS-ABORT — RegressionSeedsTest also runs this config); healthy seeds finish well under it,
        // and a genuine spin is still caught by the wall-clock deadline.
        return new SimulationConfig(seed, 5, 160, 0.7, ALL_FAULTS, Duration.ofSeconds(60));
    }

    /**
     * Returns the heaviest configuration for the chaos campaign: more {@code OrderWorkflow} instances, more steps, a
     * higher fault probability, and the full {@link #CHAOS_FAULTS} set (every fault plus the five chaos faults). The
     * chaos world drives the same mixed workload {@code SimulationWorld#defaultRegistrations()} provides (order +
     * versioned + migrating + payload + combinator + two correlated + reducer) with more order instances, so every
     * invariant is exercised under the chaos faults.
     * <p>
     * The {@code wallClockDeadline} is deliberately large (the smoke/fuzz deadlines have grown load-sensitive as the
     * instance count grew): a chaos run does many crash+recover cycles (flapping restart) over ~17 instances, so it is
     * given ample headroom so a transient CI load pause cannot false-abort it; it is still bounded so a genuine
     * hang/liveness break aborts with the enriched diagnostic instead of stalling. Like {@code fuzz}, it asserts the
     * invariants every step but does not claim bit-for-bit seed equality (the {@code ALL_FAULTS}/racy-fault precedent).
     *
     * @param seed the seed.
     * @return a chaos-sized configuration.
     */
    public static SimulationConfig chaos(long seed) {
        // Drives 23 instances (10 OrderWorkflow + the 13 singletons incl. the four P-series production-realism
        // workloads). maxSteps bumped 90->180->240 for the same workload-grew reason as smoke/fuzz (secondary
        // anti-hang cap; the 240s wall-clock deadline is the primary hang guard).
        return new SimulationConfig(seed, 10, 240, 0.85, CHAOS_FAULTS, Duration.ofSeconds(240));
    }

    /**
     * Phase 4 — swarm configuration: every dimension (instance count, step budget, fault probability, fault subset,
     * carrier/interleaving mode) is a pure function of the given {@code swarmSeed}. Where {@link #smoke(long)} /
     * {@link #chaos(long)} vary only the world seed over a FIXED shape, swarm varies the SHAPE itself — the
     * swarm-testing widening move: rare configuration corners get visited instead of hand-picked ones.
     * The world seed and the shape derive from the same swarm seed, so one number reproduces the whole run.
     *
     * @param swarmSeed seed for both the configuration shape and the world.
     * @return a reproducible, seed-shaped configuration.
     */
        public static SimulationConfig swarm(long swarmSeed) {
        var shape = new java.util.Random(swarmSeed);
        int workflowCount = 1 + shape.nextInt(8);                       // 1..8 OrderWorkflow instances
        int maxSteps = 30 + shape.nextInt(61);                          // 30..90 effective steps
        double faultProbability = 0.2 + shape.nextDouble() * 0.7;       // 0.2..0.9
        // A seed-chosen, non-empty subset of the chaos fault set (bias: keep at least three kinds).
        var kinds = new java.util.ArrayList<>(java.util.Arrays.asList(CHAOS_FAULTS));
        java.util.Collections.shuffle(kinds, shape);
        int keep = 3 + shape.nextInt(kinds.size() - 2);
        var faultKinds = kinds.subList(0, keep).toArray(new FaultKind[0]);
        boolean carrier = shape.nextBoolean();
        Long interleavingSeed = carrier && shape.nextBoolean() ? shape.nextLong() : null;
        return new SimulationConfig(swarmSeed, workflowCount, maxSteps, faultProbability, faultKinds,
                                    Duration.ofSeconds(240), carrier, interleavingSeed);
    }
}
