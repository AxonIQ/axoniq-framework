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
 * Worker crash mid-step: snapshots the committed log, drops volatile engine state (live executions, processor token,
 * in-flight timers), keeps the durable event log, then recovers over the same substrate — and asserts
 * {@code CommittedHistorySurvivesCrash} (INV-3) across the recovery.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class WorkerCrashFault implements Fault {

        @Override
    public FaultKind kind() {
        return FaultKind.WORKER_CRASH;
    }

    @Override
    public void apply(SimulationContext context) {
        List<EventMessage> beforeCrash = context.world().committedLog();
        context.record("WORKER_CRASH: committed=" + beforeCrash.size() + " events, crashing + recovering");
        context.world().crashAndRecover();
        Invariants.assertCommittedHistorySurvivesCrash(beforeCrash, context.world().committedLog());
    }
}
