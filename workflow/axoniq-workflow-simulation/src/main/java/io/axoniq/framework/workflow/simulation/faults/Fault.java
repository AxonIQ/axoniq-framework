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

/**
 * A fault the simulator can inject between steps. Each fault is chosen by the single seeded RNG and applied to the
 * {@link SimulationContext}; a seed therefore fully determines the fault sequence.
 * <p>
 * Faults are responsible for any invariant assertion that is specific to the fault (e.g. a crash fault asserts
 * {@code CommittedHistorySurvivesCrash} across the recovery it performs). The harness asserts the always-on
 * invariants after every step regardless.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public interface Fault {

    /**
     * Returns the kind of this fault, used for logging and for the seed→fault-sequence trace.
     *
     * @return the fault kind.
     */
        FaultKind kind();

    /**
     * Applies the fault to the simulation. Implementations may crash/recover the engine, reorder/delay/duplicate
     * pending events, jump the clock, or arm the write-then-vanish window.
     *
     * @param context the simulation context (world, RNG, pending events, trace).
     */
    void apply(SimulationContext context);
}
