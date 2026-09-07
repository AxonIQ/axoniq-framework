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
package io.axoniq.framework.workflow.simulation;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.EventCorrelationExactScenario;
import io.axoniq.framework.workflow.simulation.workflow.CorrelatedWaitWorkflow;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises INVARIANTS.md INV-15 ({@code EventCorrelationExact}): an {@code associate(...)}-correlated event wakes
 * EXACTLY the matching waiting instance — never a non-matching one (no cross-wakeup) — and duplicate/uncorrelated events
 * produce no spurious wait completion.
 * <p>
 * The first test drives the real engine through {@link EventCorrelationExactScenario}: two {@link CorrelatedWaitWorkflow}
 * instances wait on distinct keys (A, B); delivering A's signal wakes ONLY A (B stays waiting — no cross-wakeup), an
 * uncorrelated key wakes nobody, a duplicate of A's signal does not complete A's wait twice, B completes only on its own
 * key, and a crash + replay resolves the identical matched keys. The remaining tests are assertion pins proving
 * {@link Invariants#assertEventCorrelationExact} is correct and not trivial: a sound run (each instance matched on its
 * own key) passes; a FOREIGN matched key (a cross-wakeup) throws; a duplicate producing two wait completions throws; the
 * cross-instance interleaving of the global log (the F-2 surface, asserted per {@code workflowId}) is tolerated; an
 * out-of-scope (non-correlated prefix) instance is skipped; and a mid-flight instance (wait not yet completed) is
 * skipped.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv15EventCorrelationExactTest {

    private static final String PREFIX = "corr-";
    private static final String KEY_A = "keyA";
    private static final String KEY_B = "keyB";

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void correlatedEventWakesExactlyTheMatchingWaiter_noCrossWakeup_stableAcrossReplay() {
        EventCorrelationExactScenario.Outcome outcome = EventCorrelationExactScenario.run(0L, KEY_A, KEY_B);

        // No cross-wakeup: B did NOT wake on A's signal (the §10 anti-pattern — an associated event must NOT wake every
        // waiter).
        assertThat(outcome.bWokeOnAsSignal())
                .as("instance B (waiting on keyB) must NOT wake when only keyA's signal is delivered (no cross-wakeup)")
                .isFalse();
        // Each instance matched on its OWN key.
        assertThat(outcome.matchedKeyA())
                .as("instance A woke on its own key keyA")
                .isEqualTo(KEY_A);
        assertThat(outcome.matchedKeyB())
                .as("instance B woke on its own key keyB")
                .isEqualTo(KEY_B);
        // At-most-once: the duplicate of A's signal did not complete A's wait a second time.
        assertThat(outcome.aWaitCompletionsAfterDup())
                .as("a duplicate of an already-matched signal must not complete the wait a second time")
                .isEqualTo(1);
        // Deterministic across replay: a crash + replay resolves the SAME matched keys.
        assertThat(outcome.matchedKeyAAfterReplay())
                .as("EventCorrelationExact: replaying the same history resolves instance A's matched key unchanged")
                .isEqualTo(outcome.matchedKeyA());
        assertThat(outcome.matchedKeyBAfterReplay())
                .as("EventCorrelationExact: replaying the same history resolves instance B's matched key unchanged")
                .isEqualTo(outcome.matchedKeyB());
    }

    @Test
    void assertEventCorrelationExact_passesWhenEachInstanceMatchedItsOwnKey() {
        // Two instances, each matched on its own key (corr-a -> keyA, corr-b -> keyB). Sound — must not throw.
        var log = new ArrayList<EventMessage>();
        log.addAll(completedWaitFor("corr-a", KEY_A, KEY_A));
        log.addAll(completedWaitFor("corr-b", KEY_B, KEY_B));
        Map<String, Map<String, Object>> actual = Map.of(
                "corr-a", Map.of(CorrelatedWaitWorkflow.KEY_MATCHED_KEY, KEY_A),
                "corr-b", Map.of(CorrelatedWaitWorkflow.KEY_MATCHED_KEY, KEY_B));

        assertThatCode(() -> Invariants.assertEventCorrelationExact(log, PREFIX, actual))
                .as("each instance waking on its own correlation key is sound")
                .doesNotThrowAnyException();
    }

    @Test
    void assertEventCorrelationExact_detectsForeignMatchedKey_crossWakeup() {
        // The genuine break: instance corr-a correlates on keyA but its recorded matchedKey is keyB — an event for
        // another instance's key woke it (cross-wakeup, the §10 anti-pattern). Pins that the assertion catches it.
        var log = new ArrayList<EventMessage>(completedWaitFor("corr-a", KEY_A, KEY_B)); // ownKey=A, matchedKey=B
        Map<String, Map<String, Object>> actual = Map.of(
                "corr-a", Map.of(CorrelatedWaitWorkflow.KEY_MATCHED_KEY, KEY_B));

        assertThatThrownBy(() -> Invariants.assertEventCorrelationExact(log, PREFIX, actual))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("EventCorrelationExact")
                .hasMessageContaining("corr-a")
                .hasMessageContaining("cross-wakeup");
    }

    @Test
    void assertEventCorrelationExact_detectsDuplicateProducingTwoWaitCompletions() {
        // A duplicate signal must not complete the wait twice. Recording TWO terminal awaitSignal records for one
        // instance is the break (the correlation facet of at-most-once recording).
        var log = new ArrayList<EventMessage>();
        log.add(setupStep("corr-a", KEY_A));
        log.add(waitCompleted("corr-a"));
        log.add(waitCompleted("corr-a")); // illegal: duplicate completed the wait a second time
        log.add(recordMatch("corr-a", KEY_A));

        assertThatThrownBy(() -> Invariants.assertEventCorrelationExact(log, PREFIX, Map.of()))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("EventCorrelationExact")
                .hasMessageContaining("corr-a")
                .hasMessageContaining("at most once");
    }

    @Test
    void assertEventCorrelationExact_toleratesCrossInstanceInterleaving() {
        // Two INDEPENDENT instances, each soundly matched on its own key, interleaved in the global log. INV-15 is
        // per-instance, so corr-b's wait completion is not corr-a's — must pass even though the events interleave.
        var log = new ArrayList<EventMessage>();
        log.add(setupStep("corr-a", KEY_A));
        log.add(setupStep("corr-b", KEY_B));
        log.add(waitCompleted("corr-b"));   // B's wait completes before A's in the merged global log
        log.add(waitCompleted("corr-a"));
        log.add(recordMatch("corr-b", KEY_B));
        log.add(recordMatch("corr-a", KEY_A));
        Map<String, Map<String, Object>> actual = Map.of(
                "corr-a", Map.of(CorrelatedWaitWorkflow.KEY_MATCHED_KEY, KEY_A),
                "corr-b", Map.of(CorrelatedWaitWorkflow.KEY_MATCHED_KEY, KEY_B));

        assertThatCode(() -> Invariants.assertEventCorrelationExact(log, PREFIX, actual))
                .as("independent instances' wait completions are checked per-workflowId")
                .doesNotThrowAnyException();
    }

    @Test
    void assertEventCorrelationExact_skipsNonCorrelatedInstances() {
        // A different-prefix instance is not constrained by INV-15 — even a (hypothetical) foreign matched key on it must
        // be skipped, not flagged (INV-15 is scoped to the correlated-wait workflow's id prefix).
        var log = new ArrayList<EventMessage>(completedWaitFor("order-a", KEY_A, KEY_B)); // foreign, but out of scope
        Map<String, Map<String, Object>> actual = Map.of(
                "order-a", Map.of(CorrelatedWaitWorkflow.KEY_MATCHED_KEY, KEY_B));

        assertThatCode(() -> Invariants.assertEventCorrelationExact(log, PREFIX, actual))
                .as("non-correlated (order-) instances are out of INV-15's scope and are skipped")
                .doesNotThrowAnyException();
    }

    @Test
    void assertEventCorrelationExact_skipsInstanceStillMidFlight() {
        // The instance ran setup and is parked on its wait, but no signal has matched yet (no wait completion, no
        // matchedKey). There is nothing to cross-check yet, so it must be skipped — exactly the "uncorrelated event
        // creates no wait completion" case (no spurious completion, no false positive).
        List<EventMessage> log = List.of(setupStep("corr-a", KEY_A));

        assertThatCode(() -> Invariants.assertEventCorrelationExact(log, PREFIX, Map.of()))
                .as("an instance whose wait has not yet completed (no matched signal) is skipped")
                .doesNotThrowAnyException();
    }

    /**
     * A completed wait sequence for one instance: the {@code setup} step recording {@code ownKey}, a terminal
     * {@code awaitSignal} wait completion, and the {@code recordMatch} step recording {@code matchedKey}.
     */
    private static List<EventMessage> completedWaitFor(String workflowId, String ownKey, String matchedKey) {
        return List.of(setupStep(workflowId, ownKey), waitCompleted(workflowId), recordMatch(workflowId, matchedKey));
    }

    /**
     * The {@code setup} COMPLETED step event recording the instance's own correlation key (combine reducer), exactly
     * what the engine emits for a {@code CombineGlobalAndLocalPayloadReducer} {@code execute} step.
     */
    private static EventMessage setupStep(String workflowId, String ownKey) {
        Metadata metadata = MetadataUtils.create(workflowId, CorrelatedWaitWorkflow.STEP_SETUP, StepStatus.COMPLETED)
                                         .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD,
                                              CombineGlobalAndLocalPayloadReducer.NAME);
        return new GenericEventMessage(new MessageType(CorrelatedWaitWorkflow.STEP_SETUP),
                                       Map.of(CorrelatedWaitWorkflow.KEY_OWN_KEY, ownKey), metadata);
    }

    /**
     * A terminal (COMPLETED) record for the {@code awaitSignal} wait step — the instance's wait completed.
     */
    private static EventMessage waitCompleted(String workflowId) {
        Metadata metadata = MetadataUtils.create(workflowId, CorrelatedWaitWorkflow.STEP_AWAIT_SIGNAL,
                                                 StepStatus.COMPLETED);
        return new GenericEventMessage(new MessageType(CorrelatedWaitWorkflow.STEP_AWAIT_SIGNAL), Map.of(), metadata);
    }

    /**
     * The {@code recordMatch} COMPLETED step event recording the matched event's key (combine reducer).
     */
    private static EventMessage recordMatch(String workflowId, String matchedKey) {
        Metadata metadata = MetadataUtils.create(workflowId, CorrelatedWaitWorkflow.STEP_RECORD_MATCH,
                                                 StepStatus.COMPLETED)
                                         .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD,
                                              CombineGlobalAndLocalPayloadReducer.NAME);
        return new GenericEventMessage(new MessageType(CorrelatedWaitWorkflow.STEP_RECORD_MATCH),
                                       Map.of(CorrelatedWaitWorkflow.KEY_MATCHED_KEY, matchedKey), metadata);
    }
}
