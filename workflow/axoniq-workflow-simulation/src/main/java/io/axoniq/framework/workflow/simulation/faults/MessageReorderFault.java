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

import java.util.Collections;
import java.util.List;

/**
 * Message reorder / delay / duplicate: perturbs the queue of external events still to be delivered. One of three modes
 * is chosen by the seeded RNG:
 * <ul>
 *   <li><em>reorder</em> — swap two pending events;</li>
 *   <li><em>delay</em> — push a pending event further back by extra steps;</li>
 *   <li><em>duplicate</em> — enqueue a verbatim copy of a pending event so it is delivered twice.</li>
 * </ul>
 * Duplicate delivery is the interesting one: it stresses {@code AtMostOnceRecording} (INV-2) — the engine's
 * step-name-keyed dedup and terminal-state guards must keep a redelivered wakeup from recording a step twice.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class MessageReorderFault implements Fault {

        @Override
    public FaultKind kind() {
        return FaultKind.MESSAGE_REORDER;
    }

    @Override
    public void apply(SimulationContext context) {
        List<PendingEvent> pending = context.pendingEvents();
        if (pending.isEmpty()) {
            context.record("MESSAGE_REORDER: no pending events, no-op");
            return;
        }
        int mode = context.rng().nextInt(3);
        switch (mode) {
            case 0 -> {
                int i = context.rng().nextInt(pending.size());
                int j = context.rng().nextInt(pending.size());
                Collections.swap(pending, i, j);
                context.record("MESSAGE_REORDER: swapped pending[" + i + "] and pending[" + j + "]");
            }
            case 1 -> {
                int i = context.rng().nextInt(pending.size());
                int extra = 1 + context.rng().nextInt(3);
                pending.set(i, pending.get(i).delayedBy(extra));
                context.record("MESSAGE_REORDER: delayed pending[" + i + "] by " + extra + " steps");
            }
            default -> {
                int i = context.rng().nextInt(pending.size());
                PendingEvent dup = pending.get(i);
                pending.add(new PendingEvent(dup.event(), 0, dup.description() + " (DUPLICATE)"));
                context.record("MESSAGE_REORDER: duplicated pending[" + i + "] (" + dup.description() + ")");
            }
        }
    }
}
