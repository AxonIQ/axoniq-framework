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
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishReplyEvent;

import java.time.Duration;
import java.util.List;

import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.isTerminal;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.publishRecords;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.starts;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.stepNamesById;

// Bridges TLA+ PublishRouting.tla / MC_publish.cfg and MC_publish_nogate.cfg: AtMostOnceRecording across Crash + Rerun.
/**
 * The publish primitive across the two crash windows that matter for a step whose effect <em>is</em> its record.
 * <ul>
 *   <li>{@link #crashAfterPublishCommitted}: crash once the published event is durable and before the publisher is
 *   terminal. The recovered body re-reaches the publish with the step sourced from the log and must not publish again
 *   (the replay-skip gate, the F-7 lesson). The chain then completes.</li>
 *   <li>{@link #publishVanishesThenCrash}: the publish's commit vanishes (the F-0 write-then-vanish window). There is no
 *   effect/record divergence to have: with nothing durable the publisher never observes its step and stays parked, and
 *   the recovered body publishes again — exactly one record in the end.</li>
 * </ul>
 * ORACLE: distinct records of the publish step, responder starts, terminal statuses, the reconstructed step lists.
 * WORKLOAD: the three chain definitions, one order id, one deterministic crash (plus one armed vanish). EVIDENCE:
 * {@code isVanishArmed()} flips when the vanish consumed the commit; the crash is the harness's own. AMBIGUITY: a
 * requester left waiting after recovery because its reply landed in the crash window (the known F-16 lost-wake class,
 * not a publish property) is rescued by re-delivering the reply and reported as {@code replyRescued}. BUDGET: one seed,
 * ~15 s per probe.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class PublishCrashScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(15);
    private static final Duration SETTLE_WINDOW = Duration.ofSeconds(4);

    private PublishCrashScenario() {
    }

    /**
     * Outcome of a crash probe.
     *
     * @param requestRecords   distinct committed records of the requester's publish step (expected 1).
     * @param requestRecordsAtCrash distinct records at the moment of the crash (1 for the committed probe, 0 for the
     *                         vanish probe).
     * @param responderStarts  workflow STARTED records of the responder (expected 1).
     * @param observerStarts   workflow STARTED records of the observer (expected 1).
     * @param allTerminal      requester, responder and observer all reached a terminal status.
     * @param replyRescued     the requester needed a re-delivered reply after recovery (recorded, not asserted: the
     *                         reply is lost when the crash caught the responder mid-step — its {@code handleRequest} comes
     *                         back FAILED with {@code StepIndeterminateException} under the at-most-once guarantee and the
     *                         body terminates with {@code ctx.fail} — or when it landed inside the crash window, the F-16
     *                         lost-wake class).
     * @param responderFailed  the responder ended FAILED because the crash caught its step in flight (recorded).
     * @param vanishFired      the armed vanish consumed the publish commit (vanish probe only).
     */
    public record Outcome(int requestRecords, int requestRecordsAtCrash, int responderStarts, int observerStarts,
                          boolean allTerminal, boolean replyRescued, boolean responderFailed, boolean vanishFired) {
    }

    /**
     * Crash after the published event is durable, before the publisher's terminal status.
     *
     * @param seed    world seed.
     * @param orderId order id shared by the chain.
     * @return the outcome.
     */
    public static Outcome crashAfterPublishCommitted(long seed, String orderId) {
        var effects = new CountingEffects();
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        try (var world = new SimulationWorld(seed, EngineInstance.publishChainWorkflow(effects))) {
            world.engine().publish(new PublishChainRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the requester's published request to be durable",
                                () -> publishRecords(world.committedLog(), requester,
                                                     PublishChainWorkflow.STEP_PUBLISH_REQUEST) == 1);
            int atCrash = publishRecords(world.committedLog(), requester, PublishChainWorkflow.STEP_PUBLISH_REQUEST);
            world.crashAndRecover();
            return finish(world, orderId, atCrash, false);
        }
    }

    /**
     * The publish's commit vanishes; the publisher stays parked; the crash and recovery publish it for real.
     *
     * @param seed    world seed.
     * @param orderId order id shared by the chain.
     * @return the outcome.
     */
    public static Outcome publishVanishesThenCrash(long seed, String orderId) {
        var effects = new CountingEffects();
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        try (var world = new SimulationWorld(seed, EngineInstance.publishChainWorkflow(effects))) {
            world.eventStore().armVanishCommitFor(PublishChainWorkflow.STEP_PUBLISH_REQUEST, StepStatus.COMPLETED);
            world.engine().publish(new PublishChainRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the vanish to consume the publish commit",
                                () -> !world.eventStore().isVanishArmed());
            // Nothing durable, nothing observed: the publisher must be parked, not terminal, with no record.
            boolean parked = !Polling.await(Duration.ofSeconds(2),
                                            () -> isTerminal(world.committedLog(), requester));
            int atCrash = publishRecords(world.committedLog(), requester, PublishChainWorkflow.STEP_PUBLISH_REQUEST);
            if (!parked) {
                throw new IllegalStateException("The publisher completed although its published event vanished");
            }
            world.crashAndRecover();
            return finish(world, orderId, atCrash, true);
        }
    }

    private static Outcome finish(SimulationWorld world, String orderId, int atCrash, boolean vanishFired) {
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        String responder = PublishChainWorkflow.RESPONDER_ID_PREFIX + orderId;
        String observer = PublishChainWorkflow.OBSERVER_ID_PREFIX + orderId;
        boolean settled = Polling.await(SETTLE_WINDOW, () -> allTerminal(world, orderId));
        boolean rescued = false;
        if (!settled) {
            // The reply may have landed inside the crash window (F-16 lost-wake class). Re-deliver it once, the same
            // at-least-once assumption the fuzz horizon makes. A duplicate must not complete the wait twice.
            rescued = true;
            world.engine().publish(new PublishReplyEvent(orderId));
            if (!Polling.await(DEADLINE, () -> allTerminal(world, orderId))) {
                throw new IllegalStateException(
                        "Timed out after " + DEADLINE + " waiting for the chain to complete after the reply was "
                                + "re-delivered. live=" + world.engine().liveWorkflowIds()
                                + " log=" + String.join(" | ", world.eventStore().renderCommittedLog()));
            }
        }
        var log = world.committedLog();
        Invariants.assertAtMostOnceRecording(log);
        Invariants.assertOneInstancePerStart(log);
        Invariants.assertNoForeignStepRecorded(stepNamesById(world));
        boolean responderFailed = log.stream().anyMatch(e ->
                responder.equals(io.axoniq.framework.workflow.runtime.util.MetadataUtils.getWorkflowId(e.metadata()))
                        && io.axoniq.framework.workflow.runtime.util.MetadataUtils.getWorkflowStatus(e.metadata())
                                .filter(status -> status == io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus.FAILED)
                                .isPresent());
        return new Outcome(publishRecords(log, requester, PublishChainWorkflow.STEP_PUBLISH_REQUEST),
                           atCrash, starts(log, responder), starts(log, observer),
                           allTerminal(world, orderId), rescued, responderFailed, vanishFired);
    }

    private static boolean allTerminal(SimulationWorld world, String orderId) {
        var log = world.committedLog();
        return List.of(PublishChainWorkflow.REQUESTER_ID_PREFIX, PublishChainWorkflow.RESPONDER_ID_PREFIX,
                       PublishChainWorkflow.OBSERVER_ID_PREFIX)
                   .stream().allMatch(prefix -> isTerminal(log, prefix + orderId));
    }
}
