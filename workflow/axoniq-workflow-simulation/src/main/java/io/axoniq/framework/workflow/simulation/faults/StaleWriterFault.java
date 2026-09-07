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

import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Stale writer: lands one foreign event tagged with a live instance's {@code workflowId} in the shared durable store,
 * in the window between the two condition checks of that instance's next commit, then restores the world from that
 * store the way the next segment claim would.
 * <p>
 * This is the adversary the DCB append condition exists for, reduced to the only thing the two writers share: the
 * store. A node that stopped refreshing its claim does not observe the loss, so it keeps appending for instances
 * another node has taken over. Landing one such write is enough — from the surviving execution's side it is
 * indistinguishable from a real peer, because the condition it appends under is anchored at a marker that write is
 * now past.
 * <p>
 * What must follow is the whole point of the change: the execution holding the instance is rejected on its very next
 * append, logs the rejection, interrupts its driver and publishes nothing terminal. The instance therefore parks —
 * the ADR's accepted "two interleaved writers can both get rejected and both stop; the instance stays durable and
 * parks until the next claim restores it". The fault performs that next claim itself
 * ({@code SimulationWorld#crashAndRecover()}), so the instance resumes at a marker past the foreign write and the
 * run's liveness invariant ({@code EventuallyTerminates}, INV-5) still has to hold.
 * <p>
 * <strong>Landing evidence.</strong> Two signals, both durable: the foreign write itself is in the store's log
 * ({@code ControllableEventStorageEngine#foreignWritesInLog()}), and a fenced execution logs
 * {@code "Append of ... was rejected"} for the instance it targeted. The second one is a race — the fault cannot know
 * whether the owner had an append in flight at that moment — so a single run is not required to fence anything; a
 * campaign that never observes a rejection is inconclusive, not a pass (see {@code StaleWriterFencingScenario}).
 * <p>
 * The instance is drawn from the seeded RNG over every instance that has a committed {@code STARTED} and no committed
 * terminal record, so the fault reaches whichever feature the workload happens to have in flight — a parallel
 * combinator branch, a retry backoff, a parked wait, a payload write, a version migration — rather than one hand-picked
 * shape.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class StaleWriterFault implements Fault {

    private static final Duration ARM_BUDGET = Duration.ofMillis(300);
    private static final Duration NUDGE = Duration.ofSeconds(1);
    private static final int NUDGES = 10;

        @Override
    public FaultKind kind() {
        return FaultKind.STALE_WRITER;
    }

    @Override
    public void apply(SimulationContext context) {
        var world = context.world();
        // Two shots at the same adversary. The precise one first: arm the store so the next commit is beaten to the
        // punch in the window between its two condition checks, and nudge virtual time so the timers the instances sit
        // on produce that commit. When nothing commits while armed (the harness settles before it injects, so this is
        // the common case), fall back to landing the write blind on a live instance — the write always lands either
        // way, which is what the restore path downstream has to cope with.
        world.eventStore().armForeignWriteBeforeNextCommit();
        boolean fenced = false;
        for (int nudge = 0; nudge < NUDGES && !fenced; nudge++) {
            world.advanceTime(NUDGE);
            fenced = Polling.await(ARM_BUDGET, () -> !world.eventStore().isFenceArmed());
        }
        String workflowId;
        if (fenced) {
            workflowId = world.eventStore().fencedInstance().orElse("<unknown>");
            context.record("STALE_WRITER: foreign tagged append beat instance " + workflowId
                                   + " to its own commit; the durable log now holds "
                                   + world.eventStore().foreignWritesInLog() + " foreign write(s)");
        } else {
            world.eventStore().disarmFence();
            var live = liveInstanceIds(world.committedLog());
            if (live.isEmpty()) {
                context.record("STALE_WRITER: no live instance to write for, no-op");
                return;
            }
            workflowId = live.get(context.rng().nextInt(live.size()));
            world.eventStore().appendForeign(workflowId);
            context.record("STALE_WRITER: foreign tagged append for live instance " + workflowId
                                   + "; the durable log now holds " + world.eventStore().foreignWritesInLog()
                                   + " foreign write(s) and the owner's next append must be rejected");
        }
        // A fenced execution stops without a terminal event, and an instance whose store now holds an event it never
        // wrote is stopped the moment it appends. Only a claim un-parks either, so the fault performs one; its
        // per-instance sourcing read seeds the restored execution past the foreign write.
        world.crashAndRecover();
        context.record("STALE_WRITER: claim restored the world past the foreign write for " + workflowId);
    }

    /**
     * Returns the instances that have a committed {@code STARTED} workflow record and no committed terminal one, in
     * first-seen order so the draw is a pure function of the seed.
     */
        private static List<String> liveInstanceIds(List<EventMessage> committedLog) {
        var started = new LinkedHashSet<String>();
        var terminal = new LinkedHashSet<String>();
        for (EventMessage event : committedLog) {
            var status = MetadataUtils.getWorkflowStatus(event.metadata());
            if (status.isEmpty()) {
                continue;
            }
            var workflowId = MetadataUtils.getWorkflowId(event.metadata());
            if (status.get().isTerminal()) {
                terminal.add(workflowId);
            } else {
                started.add(workflowId);
            }
        }
        var live = new ArrayList<>(started);
        live.removeAll(terminal);
        return live;
    }

}
