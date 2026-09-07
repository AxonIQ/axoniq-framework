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
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;

/**
 * Flapping restart: several rapid {@code crashAndRecover} cycles in a row (a count drawn from the seeded RNG), modelling
 * a node that keeps dying and coming back (a crash-loop / flapping deploy) before it can make any progress. After every
 * single cycle it re-asserts {@code CommittedHistorySurvivesCrash} (INV-3) across that cycle — so a durability break in
 * any one of the rapid restarts is caught immediately rather than masked by a later recovery.
 * <p>
 * This is the multi-cycle hardening of the single-cycle {@link WorkerCrashFault} / {@link RestartFault}: it stresses
 * {@code CommittedHistorySurvivesCrash} (INV-3) and {@code EventuallyTerminates} (INV-5, resume after repeated
 * restarts). The cycle count is drawn from {@link SimulationContext#rng()}, so a seed fully determines the run.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FlappingRestartFault implements Fault {

    private static final int MIN_CYCLES = 2;
    private static final int MAX_CYCLES = 4;

        @Override
    public FaultKind kind() {
        return FaultKind.FLAPPING_RESTART;
    }

    @Override
    public void apply(SimulationContext context) {
        int cycles = MIN_CYCLES + context.rng().nextInt(MAX_CYCLES - MIN_CYCLES + 1);
        context.record("FLAPPING_RESTART: " + cycles + " rapid crash+recover cycles");
        for (int cycle = 0; cycle < cycles; cycle++) {
            List<EventMessage> beforeCycle = context.world().committedLog();
            context.world().crashAndRecover();
            // Re-assert INV-3 across EACH cycle so a durability break in any one rapid restart is caught immediately,
            // not masked by a subsequent recovery. Per-workflowId (assertCommittedHistorySurvivesCrash groups by
            // workflowId — F-2-robust).
            Invariants.assertCommittedHistorySurvivesCrash(beforeCycle, context.world().committedLog());
        }
    }
}
