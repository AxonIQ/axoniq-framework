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
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.stepNamesById;

// Bridges TLA+ PublishRouting.tla / MC_publish.cfg: WakeExactlyOnce and LateWaitNeverWakes.
/**
 * The order of a {@code waitForEvent} and the {@code publish} that answers it decides whether the wait is woken.
 * <ul>
 *   <li>{@link #waiterRegisteredBeforePublish}: a waiter parked on the published event's type <em>before</em> the
 *   publish is woken by it exactly once, and registers none of the publisher's steps.</li>
 *   <li>{@link #waiterRegisteredAfterPublish}: a waiter whose wait is registered <em>after</em> the published event
 *   went by is not woken — wait conditions are evaluated at live delivery only. That is the engine's documented
 *   semantics for every event, published or external; it is pinned here as a characterisation, not a finding.</li>
 * </ul>
 * ORACLE: the waiter's {@code awaitRequest} step records and its reconstructed step list. WORKLOAD: the chain plus one
 * waiter, one order id, no random faults. EVIDENCE: the parked wait's STARTED record is polled for before the publish;
 * the published event's record is polled for before the late waiter starts. AMBIGUITY: the "not woken" verdict is an
 * absence over a bounded window and is reported as such. BUDGET: one seed, ~10 s per probe.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class PublishWaitOrderScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(15);
    private static final Duration ABSENCE_WINDOW = Duration.ofSeconds(2);

    private PublishWaitOrderScenario() {
    }

    /**
     * Outcome of the wait-before-publish probe.
     *
     * @param waitCompletions   COMPLETED records of the waiter's wait step (expected 1).
     * @param afterRequestRuns  runs of the waiter's post-wait effect (expected 1).
     * @param waiterTerminal    the waiter reached a terminal status.
     * @param waiterHoldsForeign the waiter's state holds the requester's publish step (expected {@code false}).
     */
    public record BeforeOutcome(int waitCompletions, int afterRequestRuns, boolean waiterTerminal,
                                boolean waiterHoldsForeign) {
    }

    /**
     * Outcome of the wait-after-publish probe.
     *
     * @param publishedBeforeWait the published event was durable before the waiter registered its wait.
     * @param waiterParked        the waiter's wait step recorded STARTED.
     * @param wokenWithinWindow   the wait completed within the absence window (expected {@code false}).
     */
    public record AfterOutcome(boolean publishedBeforeWait, boolean waiterParked, boolean wokenWithinWindow) {
    }

    /**
     * Waiter first, then the publishing chain.
     *
     * @param seed    world seed.
     * @param orderId order id shared by waiter and chain.
     * @return the outcome.
     */
    public static BeforeOutcome waiterRegisteredBeforePublish(long seed, String orderId) {
        var effects = new CountingEffects();
        String waiter = PublishChainWorkflow.WAITER_ID_PREFIX + orderId;
        try (var world = new SimulationWorld(seed, registrations(effects))) {
            world.engine().publish(new PublishWaiterStartedEvent(orderId, orderId));
            Polling.awaitOrFail(DEADLINE, "the waiter to be parked on its wait step",
                                () -> hasStep(world.committedLog(), waiter, PublishChainWorkflow.STEP_AWAIT_REQUEST,
                                              StepStatus.STARTED));
            world.engine().publish(new PublishChainRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the waiter to be woken by the published request and complete",
                                () -> isTerminal(world.committedLog(), waiter));
            Invariants.assertAtMostOnceRecording(world.committedLog());
            Invariants.assertNoForeignStepRecorded(stepNamesById(world));
            return new BeforeOutcome(
                    PublishOracles.publishRecords(world.committedLog(), waiter, PublishChainWorkflow.STEP_AWAIT_REQUEST),
                    effects.count(waiter, PublishChainWorkflow.STEP_AFTER_REQUEST),
                    isTerminal(world.committedLog(), waiter),
                    stepNamesById(world).getOrDefault(waiter, List.of())
                                        .contains(PublishChainWorkflow.STEP_PUBLISH_REQUEST));
        }
    }

    /**
     * The publishing chain first, then the waiter.
     *
     * @param seed    world seed.
     * @param orderId order id shared by waiter and chain.
     * @return the outcome.
     */
    public static AfterOutcome waiterRegisteredAfterPublish(long seed, String orderId) {
        var effects = new CountingEffects();
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        String waiter = PublishChainWorkflow.WAITER_ID_PREFIX + orderId;
        try (var world = new SimulationWorld(seed, registrations(effects))) {
            world.engine().publish(new PublishChainRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the requester to have published its request and completed",
                                () -> isTerminal(world.committedLog(), requester));
            boolean publishedBeforeWait = PublishOracles.publishRecords(world.committedLog(), requester,
                                                                        PublishChainWorkflow.STEP_PUBLISH_REQUEST) == 1;
            world.engine().publish(new PublishWaiterStartedEvent(orderId, orderId));
            Polling.awaitOrFail(DEADLINE, "the late waiter to be parked on its wait step",
                                () -> hasStep(world.committedLog(), waiter, PublishChainWorkflow.STEP_AWAIT_REQUEST,
                                              StepStatus.STARTED));
            boolean woken = Polling.await(ABSENCE_WINDOW,
                                          () -> hasStep(world.committedLog(), waiter,
                                                        PublishChainWorkflow.STEP_AWAIT_REQUEST, StepStatus.COMPLETED));
            Invariants.assertNoForeignStepRecorded(stepNamesById(world));
            return new AfterOutcome(publishedBeforeWait, true, woken);
        }
    }

    private static List<EngineInstance.WorkflowRegistration> registrations(CountingEffects effects) {
        var all = new ArrayList<>(EngineInstance.publishChainWorkflow(effects));
        all.add(EngineInstance.publishWaiterWorkflow(effects));
        return all;
    }
}
