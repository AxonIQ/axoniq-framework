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
import io.axoniq.framework.workflow.simulation.harness.SimulationContext;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PaymentConfirmedEvent;
import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Duplicate-completed: re-delivers a verbatim copy of an <em>external trigger</em> event that already drove a workflow
 * to a committed <strong>terminal</strong> status — the after-the-fact duplicate delivery a flaky transport produces.
 * Where {@link MessageReorderFault}'s duplicate mode duplicates a <em>still-pending</em> trigger, this re-delivers the
 * trigger (start event or wakeup/correlation signal) of an instance that is <em>already terminal</em> in the committed
 * log.
 * <p>
 * This stresses the dedup the engine actually provides: a re-delivered <em>wakeup</em> ({@link PaymentConfirmedEvent})
 * for an instance whose {@code awaitConfirmation} wait already completed and that is now terminal must be a no-op — no
 * second terminal step record ({@code AtMostOnceRecording} INV-2) and no work after terminal ({@code TerminalIsFinal}
 * INV-7). The harness asserts INV-2/INV-7/INV-10 after the step. Which terminal instance's wakeup to re-deliver is
 * drawn from the seeded RNG.
 * <p>
 * <strong>Two deliberate scoping choices (both triage outcomes — see POC-TLA-DST.adoc chaos triage):</strong>
 * <ul>
 *   <li><em>Wakeups, not the engine's own step events</em> — re-publishing an engine-emitted internal step record (e.g.
 *       {@code ShipOrderCompleted}) straight into the sink is not a duplicate <em>delivery</em> the engine dedups; it
 *       bypasses the workflow body and lands as a fresh raw append, which over-claims a dedup the protocol never
 *       promised. <strong>Class (b)</strong> harness over-strength (chaos seed 12).</li>
 *   <li><em>Wakeups, not start events</em> — re-delivering an {@code OrderPlacedEvent} <em>start</em> of an
 *       already-terminal instance re-spawns the key (a fresh lifecycle that re-runs {@code reserveInventory}), which is
 *       the documented after-terminal re-spawn gap <strong>F-3</strong> — INV-10 tolerates it but INV-2 catches the
 *       re-run step record. <strong>Class (a)</strong> known-gap leakage (chaos seed 40); gated out here so the general
 *       chaos campaign stays green, with F-3 still covered by {@code TerminalIsFinalScenario}'s start-redelivery probe.</li>
 * </ul>
 * The realistic duplicate-delivery adversary that the engine <em>does</em> defend against — a redelivered wakeup of a
 * finished instance — is therefore what this fault exercises.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class DuplicateCompletedFault implements Fault {

        @Override
    public FaultKind kind() {
        return FaultKind.DUPLICATE_COMPLETED;
    }

    @Override
    public void apply(SimulationContext context) {
        List<EventMessage> committed = context.world().committedLog();
        // Re-deliver the WAKEUP (PaymentConfirmedEvent) of an order instance already TERMINAL in the committed log —
        // the "committed terminal workflow" the duplicate targets. Start events (OrderPlacedEvent) are deliberately NOT
        // re-delivered: a start of a terminal instance re-spawns the key (the documented F-3 after-terminal re-spawn),
        // which would re-surface that known gap (chaos seed 40). The correlated-wait instances' per-instance keys are
        // not reconstructable from the workflow id alone; their duplicate-delivery dedup is already covered by
        // MESSAGE_REORDER's duplicate mode over the corr- signals in the pending batch (DstSimulation#buildConfirmationBatch).
        var terminalIds = terminalWorkflowIds(committed);
        Object trigger = pickTrigger(terminalIds, context);
        if (trigger == null) {
            context.record("DUPLICATE_COMPLETED: no terminal order instance to re-trigger, no-op");
            return;
        }
        // Re-publish the wakeup of an already-terminal instance. A correct engine deduplicates it (the instance is
        // terminal, the awaitConfirmation wait already completed, no live waiter); the harness's per-step INV-2
        // (AtMostOnceRecording), INV-7 (TerminalIsFinal) and INV-10 (OneInstancePerStart) checks catch a duplicate
        // terminal recording or any work after terminal.
        context.world().engine().publish(trigger);
        context.record("DUPLICATE_COMPLETED: re-delivered wakeup " + trigger + " of an already-terminal instance "
                               + "(engine must dedup)");
    }

    /**
     * Picks one wakeup ({@link PaymentConfirmedEvent}) to re-deliver, drawn from the seeded RNG over the terminal order
     * instances. Start events are excluded (re-spawn = F-3); only the post-completion wakeup is re-delivered.
     */
    @Nullable
    private static Object pickTrigger(Set<String> terminalIds, SimulationContext context) {
        var triggers = new ArrayList<Object>();
        for (String workflowId : terminalIds) {
            if (workflowId.startsWith("order-")) {
                triggers.add(new PaymentConfirmedEvent(workflowId.substring("order-".length())));
            }
        }
        if (triggers.isEmpty()) {
            return null;
        }
        return triggers.get(context.rng().nextInt(triggers.size()));
    }

    /**
     * Returns the workflow ids that have a committed terminal workflow-status event.
     */
        private static Set<String> terminalWorkflowIds(List<EventMessage> committed) {
        var ids = new LinkedHashSet<String>();
        for (EventMessage event : committed) {
            var metadata = event.metadata();
            boolean terminal = MetadataUtils.getWorkflowStatus(metadata).map(s -> s.isTerminal()).orElse(false);
            if (terminal) {
                ids.add(MetadataUtils.getWorkflowId(metadata));
            }
        }
        return ids;
    }
}
