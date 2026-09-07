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

import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * Event-store latency jitter: perturbs delivery timing and ordering inside the harness's own control, without dropping
 * any engine state. Two seeded perturbations are applied together:
 * <ul>
 *   <li><em>extra settle nudges</em> — advances virtual time by a seed-chosen number of small nudges, letting parked
 *       durable-delay timers (sleeps, retry backoff) fire at slightly different points than a clean run would;</li>
 *   <li><em>aggressive batch shuffle</em> — performs several seed-chosen swaps over the still-pending external-event
 *       batch (more than {@link MessageReorderFault}'s single swap), so the cross-instance delivery order this step is
 *       jittered harder.</li>
 * </ul>
 * Stresses {@code DeterministicReplay} (INV-4) and ordering robustness: an instance's per-{@code workflowId} committed
 * subsequence must stay stable however the cross-instance global interleaving is jittered (the F-2 surface the harness
 * deliberately factors out everywhere). Every choice is drawn from the seeded RNG, so a seed fully determines the run.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class EventStoreLatencyJitterFault implements Fault {

    private static final Duration NUDGE = Duration.ofMillis(250);
    private static final int MAX_EXTRA_NUDGES = 6;
    private static final int MAX_SHUFFLE_SWAPS = 5;

        @Override
    public FaultKind kind() {
        return FaultKind.EVENT_STORE_LATENCY_JITTER;
    }

    @Override
    public void apply(SimulationContext context) {
        int extraNudges = context.rng().nextInt(MAX_EXTRA_NUDGES + 1);
        for (int i = 0; i < extraNudges; i++) {
            context.world().advanceTime(NUDGE);
        }
        List<PendingEvent> pending = context.pendingEvents();
        int swaps = 0;
        if (pending.size() > 1) {
            int desired = 1 + context.rng().nextInt(MAX_SHUFFLE_SWAPS);
            for (int i = 0; i < desired; i++) {
                int a = context.rng().nextInt(pending.size());
                int b = context.rng().nextInt(pending.size());
                Collections.swap(pending, a, b);
                swaps++;
            }
        }
        context.record("EVENT_STORE_LATENCY_JITTER: +" + extraNudges + " nudges (" + (extraNudges * NUDGE.toMillis())
                               + "ms), " + swaps + " pending-batch swaps over " + pending.size() + " pending");
    }
}
