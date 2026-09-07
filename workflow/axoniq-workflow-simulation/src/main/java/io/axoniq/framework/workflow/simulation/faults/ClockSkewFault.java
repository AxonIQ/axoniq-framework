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
package io.axoniq.framework.workflow.simulation.faults;

import io.axoniq.framework.workflow.simulation.harness.SimulationContext;

import java.time.Duration;

/**
 * Clock skew: a clock jump of a varied, seed-chosen magnitude, advancing the mutable clock and the virtual-time
 * scheduler in lock-step (so the engine's wall-clock timeout math and durable-delay firing move together, the same
 * lock-step {@link ClockJumpFault} relies on). Unlike {@link ClockJumpFault}, which always jumps 1–60 minutes, this
 * draws a magnitude across a wide range of scales — sub-second, seconds, minutes, or many hours — to exercise both
 * tiny clock wobble and large leaps in the same fault.
 * <p>
 * Only <em>forward</em> jumps are used: the {@code MutableClock} and {@code ManualWorkflowScheduler} virtual time is
 * monotonic, and the scheduler fires tasks in non-decreasing due-time order, so a backward jump would violate that
 * contract (and is not something a correct durable scheduler must tolerate). Skew is therefore modelled as forward
 * jumps of varying size rather than varying direction. Stresses {@code EventuallyTerminates} (INV-5) and the
 * {@code orTimeout} per-attempt timeout math. The magnitude is drawn from {@link SimulationContext#rng()}, so a seed
 * fully determines the run.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ClockSkewFault implements Fault {

        @Override
    public FaultKind kind() {
        return FaultKind.CLOCK_SKEW;
    }

    @Override
    public void apply(SimulationContext context) {
        // Pick a scale, then a magnitude within it, so the fault spans sub-second wobble to multi-hour leaps. All
        // forward (monotonic virtual time); the awaitConfirmation wait timeout is 365 days, far beyond any of these, so
        // a pure wait is never spuriously timed out by a skew.
        int scale = context.rng().nextInt(4);
        Duration delta = switch (scale) {
            case 0 -> Duration.ofMillis(1 + context.rng().nextInt(900));   // sub-second wobble
            case 1 -> Duration.ofSeconds(1 + context.rng().nextInt(59));   // seconds
            case 2 -> Duration.ofMinutes(1 + context.rng().nextInt(120));  // minutes to ~2h
            default -> Duration.ofHours(1 + context.rng().nextInt(24));    // 1–24h leap
        };
        context.record("CLOCK_SKEW: forward +" + delta + " (scale=" + scale + ", fires due timers)");
        context.world().advanceTime(delta);
    }
}
