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
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CorrelatedWaitWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedWaitRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Deterministic cross-wakeup scenario for INVARIANTS.md INV-15 ({@code EventCorrelationExact}): two
 * {@link CorrelatedWaitWorkflow} instances waiting on DISTINCT correlation keys (A and B) in ONE engine, driven so the
 * heart of the property is observable — an {@code associate(...)}-correlated event wakes EXACTLY the matching waiting
 * instance, never a non-matching one (no cross-wakeup), and duplicate/uncorrelated events produce no spurious wait
 * completion. This is the implementation-level twin of the engine's §10 anti-pattern guard ("an associated event must
 * NOT wake every waiter").
 * <p>
 * Both instances register under one {@link EngineInstance#correlatedWaitWorkflow} definition (a single
 * {@code waitForEvent} body), each correlating on its own per-instance key carried on the start event. Steps:
 * <ol>
 *   <li>start instance A (key {@code keyA}) and instance B (key {@code keyB}); both run their {@code setup} step and
 *       suspend on their key-correlated {@code awaitSignal} wait — both LIVE (STARTED), neither matched;</li>
 *   <li>deliver {@code CorrelatedSignalEvent(key=keyA)} → ONLY instance A's wait completes (matchedKey=keyA); instance B
 *       stays STARTED on its wait — <strong>no cross-wakeup</strong>;</li>
 *   <li>deliver an UNCORRELATED {@code CorrelatedSignalEvent(key=keyZ)} (no waiter) → no instance wakes, no spurious
 *       step/instance recorded;</li>
 *   <li>deliver a DUPLICATE {@code CorrelatedSignalEvent(key=keyA)} → A's wait does not complete a second time (still
 *       exactly one wait COMPLETED for A);</li>
 *   <li>deliver {@code CorrelatedSignalEvent(key=keyB)} → now B completes (matchedKey=keyB);</li>
 *   <li>crash + replay → the matched-key resolution is byte-for-byte unchanged (deterministic across replay, jointly
 *       with INV-4).</li>
 * </ol>
 * {@link Invariants#assertEventCorrelationExact} must hold at every observation point, and the {@link Outcome} captures
 * the observables the test pins (each instance's matched key, B not woken by A's signal, at-most-once wait completion,
 * the replay-stable matched keys).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class EventCorrelationExactScenario {

    private static final String PREFIX = "corr-";

    private EventCorrelationExactScenario() {
    }

    /**
     * Result of running the cross-wakeup scenario.
     *
     * @param matchedKeyA                 instance A's recorded matched key after delivering its signal (must equal
     *                                    {@code keyA} — A woke on its own signal).
     * @param matchedKeyB                 instance B's recorded matched key after delivering its signal (must equal
     *                                    {@code keyB} — B woke on its own signal).
     * @param bWokeOnAsSignal             whether instance B's wait completed after delivering only A's signal (must be
     *                                    {@code false} — no cross-wakeup; B stays waiting until its own key arrives).
     * @param aWaitCompletionsAfterDup    how many times instance A's wait step recorded a terminal outcome after the
     *                                    duplicate A signal (must be exactly 1 — the duplicate does not complete it
     *                                    again).
     * @param matchedKeyAAfterReplay      instance A's recorded matched key after a crash + replay (must equal
     *                                    {@code matchedKeyA} — replay-stable).
     * @param matchedKeyBAfterReplay      instance B's recorded matched key after a crash + replay (must equal
     *                                    {@code matchedKeyB} — replay-stable).
     */
    public record Outcome(String matchedKeyA, String matchedKeyB, boolean bWokeOnAsSignal,
                          int aWaitCompletionsAfterDup, String matchedKeyAAfterReplay,
                          String matchedKeyBAfterReplay) {

    }

    /**
     * Runs the cross-wakeup scenario against a fresh world registering {@link CorrelatedWaitWorkflow} and returns what
     * it observed.
     *
     * @param seed seed for the world's deterministic id source.
     * @param keyA correlation key for the first instance.
     * @param keyB correlation key for the second instance (distinct from {@code keyA}).
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String keyA, String keyB) {
        var registration = EngineInstance.correlatedWaitWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowA = PREFIX + "a";
            String workflowB = PREFIX + "b";

            // 1. Start both instances; each runs setup then suspends on its key-correlated wait. Wait until both are
            // genuinely PARKED on their wait step (its STARTED record committed) so the signals below land on a
            // registered waiter — a signal that arrives before the waiter is registered is simply dropped (no waiter).
            world.engine().publish(new CorrelatedWaitRequestedEvent("a", keyA));
            world.engine().publish(new CorrelatedWaitRequestedEvent("b", keyB));
            Polling.awaitOrFail(Duration.ofSeconds(10), "both correlated-wait instances to be parked on their wait step",
                                () -> hasStep(world.committedLog(), workflowA, CorrelatedWaitWorkflow.STEP_AWAIT_SIGNAL)
                                        && hasStep(world.committedLog(), workflowB,
                                                   CorrelatedWaitWorkflow.STEP_AWAIT_SIGNAL));
            Invariants.assertEventCorrelationExact(world.committedLog(), PREFIX,
                                                   world.engine().reconstructedPayloads(PREFIX));

            // 2. Deliver A's signal. ONLY A must wake (matchedKey=keyA); B must stay STARTED on its wait — no
            // cross-wakeup (the §10 anti-pattern: an associated event must NOT wake every waiter).
            world.engine().publish(new CorrelatedSignalEvent(keyA));
            Polling.awaitOrFail(Duration.ofSeconds(10), "instance A to complete its wait on keyA",
                                () -> waitCompletions(world.committedLog(), workflowA) == 1);
            boolean bWokeOnAsSignal = waitCompletions(world.committedLog(), workflowB) > 0;
            Invariants.assertEventCorrelationExact(world.committedLog(), PREFIX,
                                                   world.engine().reconstructedPayloads(PREFIX));

            // 3. Deliver an UNCORRELATED signal (no instance waits on keyZ). Nobody must wake; no spurious record.
            world.engine().publish(new CorrelatedSignalEvent("keyZ"));
            // 4. Deliver a DUPLICATE of A's signal. A's wait must NOT complete a second time (correlation facet of
            // at-most-once recording — the engine's step-name dedup + terminal-state guards hold the line).
            world.engine().publish(new CorrelatedSignalEvent(keyA));
            // Give the (uncorrelated + duplicate) signals a bounded window to (wrongly) wake B or re-complete A.
            Polling.await(Duration.ofSeconds(2),
                          () -> waitCompletions(world.committedLog(), workflowB) > 0
                                  || waitCompletions(world.committedLog(), workflowA) > 1);
            int aWaitCompletionsAfterDup = waitCompletions(world.committedLog(), workflowA);
            Invariants.assertEventCorrelationExact(world.committedLog(), PREFIX,
                                                   world.engine().reconstructedPayloads(PREFIX));

            // 5. Deliver B's own signal. NOW B completes (matchedKey=keyB). Wait until B's matched key is projected
            // (the recordMatch step commits after the wait completes), so the read below is non-empty.
            world.engine().publish(new CorrelatedSignalEvent(keyB));
            Polling.awaitOrFail(Duration.ofSeconds(10), "instance B to record its matched key keyB",
                                () -> !matchedKey(world, workflowB).isEmpty());
            String matchedKeyA = matchedKey(world, workflowA);
            String matchedKeyB = matchedKey(world, workflowB);
            Invariants.assertEventCorrelationExact(world.committedLog(), PREFIX,
                                                   world.engine().reconstructedPayloads(PREFIX));

            // 6. IN-SCOPE INV-15 (deterministic across replay): a crash + replay must resolve the SAME matched keys.
            // Replay re-runs the (already-recorded) bodies and emits nothing new; the matched keys must be unchanged.
            world.crashAndRecover();
            Polling.await(Duration.ofSeconds(2),
                          () -> !matchedKey(world, workflowA).equals(matchedKeyA)
                                  || !matchedKey(world, workflowB).equals(matchedKeyB));
            String matchedKeyAAfterReplay = matchedKey(world, workflowA);
            String matchedKeyBAfterReplay = matchedKey(world, workflowB);
            Invariants.assertEventCorrelationExact(world.committedLog(), PREFIX,
                                                   world.engine().reconstructedPayloads(PREFIX));

            return new Outcome(matchedKeyA, matchedKeyB, bWokeOnAsSignal, aWaitCompletionsAfterDup,
                               matchedKeyAAfterReplay, matchedKeyBAfterReplay);
        }
    }

    /**
     * The recorded matched key for {@code workflowId}, read from the engine's reconstructed payload (the history
     * read-model's {@code state().payload()}); empty string if not yet recorded.
     */
        private static String matchedKey(SimulationWorld world, String workflowId) {
        Object key = world.engine().reconstructedPayloads(PREFIX).getOrDefault(workflowId, Map.of())
                          .get(CorrelatedWaitWorkflow.KEY_MATCHED_KEY);
        return key == null ? "" : String.valueOf(key);
    }

    /**
     * How many terminal records the instance's {@code awaitSignal} wait step has in the committed log.
     */
    private static int waitCompletions(List<EventMessage> committedLog, String workflowId) {
        return (int) committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                .filter(e -> CorrelatedWaitWorkflow.STEP_AWAIT_SIGNAL.equals(MetadataUtils.getStepName(e.metadata())))
                .filter(e -> MetadataUtils.getStepStatus(e.metadata()).map(StepStatus::isTerminal).orElse(false))
                .count();
    }

    private static boolean hasStep(List<EventMessage> committedLog, String workflowId,
                                   String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).isPresent()
                        && stepName.equals(MetadataUtils.getStepName(e.metadata())));
    }
}
