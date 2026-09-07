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
 * Phase 4 chaos fault — {@link FaultKind#DUPLICATED_APPEND}: arms the durable store to record the next commit
 * TWICE in the recovery log (an at-least-once store: a retried append whose first attempt actually landed). The
 * live path is unchanged; the duplicate surfaces on the next crash+recovery replay, stressing the
 * replay-idempotence face of {@code AtMostOnceRecording} (INV-2) and {@code DeterministicReplay} (INV-4) — the
 * engine's {@code evolve} must treat the duplicated durable event as the no-op re-apply it already claims to be.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class DuplicatedAppendFault implements Fault {

        @Override
    public FaultKind kind() {
        return FaultKind.DUPLICATED_APPEND;
    }

    @Override
    public void apply(SimulationContext context) {
        if (context.world().eventStore().isDuplicateArmed()) {
            context.record("DUPLICATED_APPEND: already armed — no-op");
            return;
        }
        context.world().eventStore().armDuplicateNextCommit();
        context.record("DUPLICATED_APPEND: armed — next durable commit will be recorded twice in the recovery log");
    }
}
