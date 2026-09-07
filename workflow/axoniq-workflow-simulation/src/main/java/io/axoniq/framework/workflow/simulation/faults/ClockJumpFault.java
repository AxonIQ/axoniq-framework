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
 * Clock jump: advances the mutable clock and the virtual-time scheduler in lock-step by a large, seed-chosen delta
 * (1–60 simulated minutes), firing every pending wait timeout and retry-backoff continuation whose due time is
 * reached. Stresses {@code EventuallyTerminates} (INV-5) and the {@code orTimeout} per-attempt timeout math, which is
 * measured against this same clock.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ClockJumpFault implements Fault {

        @Override
    public FaultKind kind() {
        return FaultKind.CLOCK_JUMP;
    }

    @Override
    public void apply(SimulationContext context) {
        int minutes = 1 + context.rng().nextInt(60);
        var delta = Duration.ofMinutes(minutes);
        context.record("CLOCK_JUMP: +" + minutes + "m (fires due timers)");
        context.world().advanceTime(delta);
    }
}
