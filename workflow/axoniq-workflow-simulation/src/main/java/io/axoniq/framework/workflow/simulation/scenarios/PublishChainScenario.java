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
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishRequestEvent;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.List;

import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.isTerminal;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.publishRecords;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.recordType;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.starts;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.stepNamesById;

// Bridges TLA+ PublishRouting.tla / MC_publish.cfg: NoForeignStepRecorded (INV-29), PublisherObservesOwnPublish
// (INV-30), SpawnAtMostOnce and WakeExactlyOnce on the happy path, then across a crash + replay.
/**
 * The publish-primitive happy path, end to end and across a crash: the requester registers its wait, publishes the
 * request through {@code ctx.awaitPublish}, the single published event starts the responder <em>and</em> the observer,
 * the responder publishes the reply the same way, the reply wakes the requester's pre-registered wait.
 * <p>
 * ORACLE: per chain instance, the durable log and the engine's reconstructed state. Exactly one distinct record per
 * publish step, under the business event's own type; the publisher's state holds its publish step; no other instance's
 * state holds it; responder and observer spawned exactly once; every counting effect ran once. Re-read after a crash
 * and recovery. WORKLOAD: the three chain definitions, one order id, no random faults (one deterministic crash).
 * EVIDENCE: the crash is the harness's own {@code crashAndRecover}. AMBIGUITY: a chain that does not settle within the
 * deadline fails the run (it is a liveness break, not an unknown). BUDGET: one seed, ~10 s.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class PublishChainScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(15);

    private PublishChainScenario() {
    }

    /**
     * What the chain produced.
     *
     * @param requestRecords          distinct committed records of the requester's publish step (expected 1).
     * @param replyRecords            distinct committed records of the responder's publish step (expected 1).
     * @param requestRecordType       qualified type of the request record (expected the business event's own type).
     * @param replyRecordType         qualified type of the reply record.
     * @param responderStarts         workflow STARTED records of the responder (expected 1: one published event, one
     *                                start).
     * @param observerStarts          workflow STARTED records of the observer (expected 1).
     * @param requesterHoldsPublish   the requester's reconstructed state holds its publish step.
     * @param responderHoldsRequest   the responder's state holds the requester's publish step (a foreign step; expected
     *                                {@code false}).
     * @param observerHoldsRequest    the observer's state holds the requester's publish step (expected {@code false}).
     * @param requesterHoldsReply     the requester's state holds the responder's publish step (expected {@code false}).
     * @param requesterWaitCompleted  the requester's pre-registered wait completed on the published reply.
     * @param effectsOnce             every counting effect of the chain ran exactly once.
     * @param stableAfterCrash        all of the above still holds after a crash and recovery.
     */
    public record Outcome(int requestRecords, int replyRecords,
                          @Nullable String requestRecordType, @Nullable String replyRecordType,
                          int responderStarts, int observerStarts,
                          boolean requesterHoldsPublish, boolean responderHoldsRequest, boolean observerHoldsRequest,
                          boolean requesterHoldsReply, boolean requesterWaitCompleted, boolean effectsOnce,
                          boolean stableAfterCrash) {
    }

    /**
     * Runs the chain for one order id.
     *
     * @param seed    world seed.
     * @param orderId order id shared by the chain.
     * @return the outcome.
     */
    public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        String responder = PublishChainWorkflow.RESPONDER_ID_PREFIX + orderId;
        String observer = PublishChainWorkflow.OBSERVER_ID_PREFIX + orderId;
        try (var world = new SimulationWorld(seed, EngineInstance.publishChainWorkflow(effects))) {
            world.engine().publish(new PublishChainRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the requester, responder and observer to all reach a terminal status",
                                () -> isTerminal(world.committedLog(), requester)
                                        && isTerminal(world.committedLog(), responder)
                                        && isTerminal(world.committedLog(), observer));
            var live = observe(world, effects, orderId);
            assertInvariants(world, orderId);

            // Crash after the whole chain is durable: recovery re-sources every instance from the log and the history
            // read-model is rebuilt by replay. Nothing may change: no re-publish, no new start, no foreign step.
            world.crashAndRecover();
            Polling.await(Duration.ofSeconds(2), () -> false); // absence window for any spurious re-publish
            var recovered = observe(world, effects, orderId);
            assertInvariants(world, orderId);
            boolean stable = live.equals(recovered);
            return new Outcome(recovered.requestRecords(), recovered.replyRecords(), recovered.requestRecordType(),
                               recovered.replyRecordType(), recovered.responderStarts(), recovered.observerStarts(),
                               recovered.requesterHoldsPublish(), recovered.responderHoldsRequest(),
                               recovered.observerHoldsRequest(), recovered.requesterHoldsReply(),
                               recovered.requesterWaitCompleted(), recovered.effectsOnce(), stable);
        }
    }

    private static Outcome observe(SimulationWorld world, CountingEffects effects, String orderId) {
        var log = world.committedLog();
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        String responder = PublishChainWorkflow.RESPONDER_ID_PREFIX + orderId;
        String observer = PublishChainWorkflow.OBSERVER_ID_PREFIX + orderId;
        var steps = stepNamesById(world);
        boolean effectsOnce = effects.count(requester, PublishChainWorkflow.STEP_PREPARE) == 1
                && effects.count(requester, PublishChainWorkflow.STEP_FINALIZE) == 1
                && effects.count(responder, PublishChainWorkflow.STEP_HANDLE_REQUEST) == 1
                && effects.count(observer, PublishChainWorkflow.STEP_OBSERVE) == 1;
        return new Outcome(
                publishRecords(log, requester, PublishChainWorkflow.STEP_PUBLISH_REQUEST),
                publishRecords(log, responder, PublishChainWorkflow.STEP_PUBLISH_REPLY),
                recordType(log, requester, PublishChainWorkflow.STEP_PUBLISH_REQUEST),
                recordType(log, responder, PublishChainWorkflow.STEP_PUBLISH_REPLY),
                starts(log, responder),
                starts(log, observer),
                steps.getOrDefault(requester, List.of()).contains(PublishChainWorkflow.STEP_PUBLISH_REQUEST),
                steps.getOrDefault(responder, List.of()).contains(PublishChainWorkflow.STEP_PUBLISH_REQUEST),
                steps.getOrDefault(observer, List.of()).contains(PublishChainWorkflow.STEP_PUBLISH_REQUEST),
                steps.getOrDefault(requester, List.of()).contains(PublishChainWorkflow.STEP_PUBLISH_REPLY),
                PublishOracles.hasStep(log, requester, PublishChainWorkflow.STEP_AWAIT_REPLY, StepStatus.COMPLETED),
                effectsOnce,
                true);
    }

    private static void assertInvariants(SimulationWorld world, String orderId) {
        var log = world.committedLog();
        var steps = stepNamesById(world);
        Invariants.assertAtMostOnceRecording(log);
        Invariants.assertOneInstancePerStart(log);
        Invariants.assertTerminalIsFinal(log);
        Invariants.assertNoForeignStepRecorded(steps);
        Invariants.assertPublisherObservesOwnPublish(log, PublishChainWorkflow.REQUESTER_ID_PREFIX,
                                                     PublishChainWorkflow.STEP_PUBLISH_REQUEST,
                                                     world.engine().messageTypeOf(new PublishRequestEvent(orderId)).qualifiedName(),
                                                     steps);
        Invariants.assertPublisherObservesOwnPublish(log, PublishChainWorkflow.RESPONDER_ID_PREFIX,
                                                     PublishChainWorkflow.STEP_PUBLISH_REPLY,
                                                     world.engine().messageTypeOf(new PublishReplyEvent(orderId)).qualifiedName(),
                                                     steps);
    }
}
