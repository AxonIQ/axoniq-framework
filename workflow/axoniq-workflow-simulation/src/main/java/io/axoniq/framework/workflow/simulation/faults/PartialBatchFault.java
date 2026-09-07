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
import io.axoniq.framework.workflow.simulation.harness.SimulationContext.PendingEvent;

import java.util.List;

/**
 * Partial batch: delivers only the head of this step's pending external-event batch, deferring a seed-chosen tail to a
 * later step. Where {@link MessageReorderFault}'s delay mode pushes a single entry back, this defers a whole tail of the
 * batch at once — modelling a delivery layer that hands the engine only part of a batch this round.
 * <p>
 * It works through the same pending-batch / delivery machinery every other event-game fault uses: each deferred entry
 * is re-stamped with a small extra delay so the harness's {@code buildConfirmationBatch} carries it (aged) to a later
 * step. Stresses {@code AtMostOnceRecording} (INV-2) and ordering robustness — a step that sees only part of its inputs
 * now (and the rest later) must still record each input's effect at most once, and per-{@code workflowId} order must
 * stay stable. The split point and per-entry deferral are drawn from {@link SimulationContext#rng()}, so a seed fully
 * determines the run.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class PartialBatchFault implements Fault {

        @Override
    public FaultKind kind() {
        return FaultKind.PARTIAL_BATCH;
    }

    @Override
    public void apply(SimulationContext context) {
        List<PendingEvent> pending = context.pendingEvents();
        // Only the entries due this step (delaySteps == 0) are deliverable now; deferring already-delayed ones is a
        // no-op. Need at least two deliverable entries to make a partial split meaningful.
        long dueCount = pending.stream().filter(p -> p.delaySteps() == 0).count();
        if (dueCount < 2) {
            context.record("PARTIAL_BATCH: <2 deliverable pending events, no-op");
            return;
        }
        // Deliver the first `deliverNow` due entries; defer the remaining due tail by a small seed-chosen delay each.
        int deliverNow = 1 + context.rng().nextInt((int) dueCount - 1);
        int seenDue = 0;
        int deferred = 0;
        for (int i = 0; i < pending.size(); i++) {
            PendingEvent entry = pending.get(i);
            if (entry.delaySteps() != 0) {
                continue;
            }
            seenDue++;
            if (seenDue > deliverNow) {
                int extra = 1 + context.rng().nextInt(3);
                pending.set(i, entry.delayedBy(extra));
                deferred++;
            }
        }
        context.record("PARTIAL_BATCH: delivering " + deliverNow + " of " + dueCount + " due events now, deferred "
                               + deferred + " to a later step");
    }
}
