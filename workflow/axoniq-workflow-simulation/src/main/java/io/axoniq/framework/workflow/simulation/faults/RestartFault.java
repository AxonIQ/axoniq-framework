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
 * Clean restart: a graceful stop + recover from the event store, with no other perturbation. Like
 * {@link WorkerCrashFault} it asserts {@code CommittedHistorySurvivesCrash} (INV-3); it differs only in intent — it
 * models a planned redeploy rather than a mid-step crash, so it exercises the resume-after-restart liveness path
 * cleanly.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class RestartFault implements Fault {

        @Override
    public FaultKind kind() {
        return FaultKind.RESTART;
    }

    @Override
    public void apply(SimulationContext context) {
        List<EventMessage> beforeRestart = context.world().committedLog();
        context.record("RESTART: committed=" + beforeRestart.size() + " events, clean restart");
        context.world().crashAndRecover();
        Invariants.assertCommittedHistorySurvivesCrash(beforeRestart, context.world().committedLog());
    }
}
