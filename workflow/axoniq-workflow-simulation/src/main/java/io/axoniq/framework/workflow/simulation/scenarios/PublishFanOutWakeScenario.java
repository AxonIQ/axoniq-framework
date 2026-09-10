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
package io.axoniq.framework.workflow.simulation.scenarios;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.PublishChainWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishChainRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishWaiterStartedEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.hasStep;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.isTerminal;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.publishRecords;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.stepNamesById;

// Bridges TLA+ PublishRouting.tla / MC_publish.cfg: WakeExactlyOnce over the Waiters set (a waiter on the
// publisher's own segment and one on a third), NoForeignStepRecorded.
/**
 * Running-to-running communication through the publish primitive, 1:N. Three workflow instances are already live and
 * parked on {@code awaitEvent} for the request of one order id, and a fourth is parked on a <em>different</em> order
 * id, when a fifth running workflow publishes that request through {@code ctx.awaitPublish}. The one published event
 * must wake each of the three exactly once, must not touch the fourth (INV-15 correlation across the publish path), and
 * none of the woken instances may register the publisher's step (INV-29). A crash after everything is durable must
 * change none of it.
 * <p>
 * ORACLE: per waiter, the {@code awaitRequest} COMPLETED records (distinct identifiers) and the post-wait effect count;
 * the control waiter's absence of a completion over a bounded window; the reconstructed step lists. WORKLOAD: the three
 * chain definitions plus the waiter definition, four waiter instances, one order id, one deterministic crash. EVIDENCE:
 * every waiter's parked STARTED record is polled for before the publisher is started, so no wake can be lost to a
 * registration race. AMBIGUITY: the control waiter's verdict is an absence over a bounded window and is reported as such.
 * BUDGET: one seed, ~15 s.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class PublishFanOutWakeScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(15);
    private static final Duration ABSENCE_WINDOW = Duration.ofSeconds(2);
    private static final List<String> WAITERS = List.of("a", "b", "c");

    private PublishFanOutWakeScenario() {
    }

    /**
     * What the fan-out produced.
     *
     * @param waitCompletionsPerWaiter COMPLETED records of the wait step, one entry per waiter (expected all 1).
     * @param afterRequestRunsPerWaiter runs of each waiter's post-wait effect (expected all 1).
     * @param allWaitersTerminal        the three waiters reached a terminal status.
     * @param controlWaiterWoken        the waiter on another order id completed its wait (expected {@code false}).
     * @param anyWaiterHoldsForeign     any waiter's state holds the publisher's step (expected {@code false}).
     * @param publisherTerminal         the publishing requester itself completed.
     * @param requestRecords            distinct records of the publisher's publish step (expected 1).
     * @param stableAfterCrash          all counts identical after a crash and recovery.
     */
    public record Outcome(List<Integer> waitCompletionsPerWaiter, List<Integer> afterRequestRunsPerWaiter,
                          boolean allWaitersTerminal, boolean controlWaiterWoken, boolean anyWaiterHoldsForeign,
                          boolean publisherTerminal, int requestRecords, boolean stableAfterCrash) {
    }

    /**
     * Runs the fan-out for one order id.
     *
     * @param seed    world seed.
     * @param orderId the order id the three waiters await and the publisher publishes.
     * @return the outcome.
     */
    public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        String control = "ctrl-" + orderId;
        String controlWaiter = PublishChainWorkflow.WAITER_ID_PREFIX + control;
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        var all = new ArrayList<>(EngineInstance.publishChainWorkflow(effects));
        all.add(EngineInstance.publishWaiterWorkflow(effects));
        try (var world = new SimulationWorld(seed, all)) {
            // 1. Three waiters on this order id and one control waiter on another, all live and parked.
            for (String w : WAITERS) {
                world.engine().publish(new PublishWaiterStartedEvent(w + "-" + orderId, orderId));
            }
            world.engine().publish(new PublishWaiterStartedEvent(control, "other-" + orderId));
            Polling.awaitOrFail(DEADLINE, "all four waiters to be parked on their wait step",
                                () -> waiterIds(orderId).stream().allMatch(id -> parked(world, id))
                                        && parked(world, controlWaiter));

            // 2. A fifth running workflow publishes the request the three are waiting for.
            world.engine().publish(new PublishChainRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the three waiters and the publisher to complete",
                                () -> waiterIds(orderId).stream().allMatch(id -> isTerminal(world.committedLog(), id))
                                        && isTerminal(world.committedLog(), requester));
            boolean controlWoken = Polling.await(ABSENCE_WINDOW,
                                                 () -> hasStep(world.committedLog(), controlWaiter,
                                                               PublishChainWorkflow.STEP_AWAIT_REQUEST,
                                                               StepStatus.COMPLETED));
            var live = observe(world, effects, orderId, controlWoken);
            assertInvariants(world);

            // 3. Crash after everything is durable: nothing may change.
            world.crashAndRecover();
            Polling.await(ABSENCE_WINDOW, () -> false);
            var recovered = observe(world, effects, orderId, controlWoken);
            assertInvariants(world);
            return new Outcome(recovered.waitCompletionsPerWaiter(), recovered.afterRequestRunsPerWaiter(),
                               recovered.allWaitersTerminal(), recovered.controlWaiterWoken(),
                               recovered.anyWaiterHoldsForeign(), recovered.publisherTerminal(),
                               recovered.requestRecords(), live.equals(recovered));
        }
    }

    private static List<String> waiterIds(String orderId) {
        return WAITERS.stream().map(w -> PublishChainWorkflow.WAITER_ID_PREFIX + w + "-" + orderId).toList();
    }

    private static boolean parked(SimulationWorld world, String waiterId) {
        return hasStep(world.committedLog(), waiterId, PublishChainWorkflow.STEP_AWAIT_REQUEST, StepStatus.STARTED);
    }

    private static Outcome observe(SimulationWorld world, CountingEffects effects, String orderId,
                                   boolean controlWoken) {
        var log = world.committedLog();
        var steps = stepNamesById(world);
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        var ids = waiterIds(orderId);
        return new Outcome(
                ids.stream().map(id -> publishRecords(log, id, PublishChainWorkflow.STEP_AWAIT_REQUEST)).toList(),
                ids.stream().map(id -> effects.count(id, PublishChainWorkflow.STEP_AFTER_REQUEST)).toList(),
                ids.stream().allMatch(id -> isTerminal(log, id)),
                controlWoken,
                ids.stream().anyMatch(id -> steps.getOrDefault(id, List.of())
                                                 .contains(PublishChainWorkflow.STEP_PUBLISH_REQUEST)),
                isTerminal(log, requester),
                publishRecords(log, requester, PublishChainWorkflow.STEP_PUBLISH_REQUEST),
                true);
    }

    private static void assertInvariants(SimulationWorld world) {
        Invariants.assertAtMostOnceRecording(world.committedLog());
        Invariants.assertOneInstancePerStart(world.committedLog());
        Invariants.assertNoForeignStepRecorded(stepNamesById(world));
    }
}
