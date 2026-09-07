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
package io.axoniq.framework.workflow.simulation.workflow;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedSignalEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * A workflow that runs a setup {@code execute} step, then suspends on a {@code waitForEvent} correlated on its
 * <strong>own</strong> per-instance key, and finally records the matched event's key into its payload — used to exercise
 * INVARIANTS.md INV-15 ({@code EventCorrelationExact}): an {@code associate(...)}-correlated event wakes EXACTLY the
 * matching waiting instance (never a non-matching one — no cross-wakeup), and duplicate/uncorrelated events produce no
 * spurious wait completion.
 * <p>
 * Shape (mirrors the {@code OrderWorkflow.awaitConfirmation} correlation pattern, axon-flow-workflow skill §4 / §7.5):
 * <ol>
 *   <li>{@link #STEP_SETUP} ({@code execute}) — a counting side effect, so there is a real step record before the wait;
 *       it also writes the instance's own correlation key into the payload under {@link #KEY_OWN_KEY} so the committed
 *       log records which key this instance correlates on;</li>
 *   <li>{@link #STEP_AWAIT_SIGNAL} ({@code waitForEvent} on {@link CorrelatedSignalEvent}, correlated via
 *       {@code associate(payloadProperty("key"), equalsTo(ownKey))}) — suspends the instance until a
 *       {@code CorrelatedSignalEvent} whose {@code key} equals THIS instance's key arrives. An effectively-infinite
 *       365-day wait timeout keeps the wait robust to the harness's virtual-time nudges and to the {@code CLOCK_JUMP}
 *       fault (it is a pure wait-for-event, not a race against the clock — exactly as {@code OrderWorkflow} does);</li>
 *   <li>{@link #STEP_RECORD_MATCH} ({@code execute} + {@link CombineGlobalAndLocalPayloadReducer#INSTANCE}) — records the
 *       <em>matched</em> event's key into the payload under {@link #KEY_MATCHED_KEY}. Because the {@code waitForEvent}
 *       returns the matched {@link CorrelatedSignalEvent} payload, this captures the key of the event that actually woke
 *       the instance: the committed log + the engine's reconstructed payload then reveal which key woke it. INV-15
 *       cross-checks {@code matchedKey == ownKey} for every instance that completed its wait — a foreign key is a
 *       cross-wakeup break.</li>
 * </ol>
 * Determinism (axon-flow-workflow skill §3.3): the body reads only its own payload (the workflow id, its correlation
 * key) and the matched event's key — no wall-clock, randomness, or external state — so the committed subsequence is a
 * pure function of the history and a replay rebuilds the identical {@code matchedKey} (the replay-stability facet INV-15
 * shares with INV-4 {@code DeterministicReplay}). The {@code execute} bodies also bump the shared {@link CountingEffects}
 * counter like the other simulation workflows; INV-15 itself reads only the recorded {@code matchedKey} vs the recorded
 * {@code ownKey}, never the effect counters.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class CorrelatedWaitWorkflow {

    /**
     * Logical workflow name (single definition), stable and distinct from the other simulation workflows.
     */
    public static final String WORKFLOW_NAME = "CorrelatedWaitWorkflow";

    /**
     * The setup step that runs (a counting side effect) and records the instance's own correlation key before the wait.
     */
    public static final String STEP_SETUP = "setup";

    /**
     * The wait step that suspends the instance until a {@link CorrelatedSignalEvent} correlated on its own key arrives.
     */
    public static final String STEP_AWAIT_SIGNAL = "awaitSignal";

    /**
     * The step that records the matched event's key into the payload (so the committed log reveals which key woke this
     * instance — its own key in a sound run, a foreign key only on a cross-wakeup break).
     */
    public static final String STEP_RECORD_MATCH = "recordMatch";

    /**
     * Payload key the setup step writes the instance's own correlation key under (the key its {@code waitForEvent}
     * associates on).
     */
    public static final String KEY_OWN_KEY = "ownKey";

    /**
     * Payload key the record step writes the matched event's key under (the key of the {@link CorrelatedSignalEvent}
     * that actually woke this instance's wait). INV-15 requires {@code matchedKey == ownKey}.
     */
    public static final String KEY_MATCHED_KEY = "matchedKey";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every {@code execute} body bumps a counter here.
     */
    public CorrelatedWaitWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The body: run the setup step (recording the instance's own key), then suspend on a {@code waitForEvent} correlated
     * on that key, then record the matched event's key. Re-run from the start on every (re)execution; on replay the
     * recorded {@code setup} returns its cached result and, once the wait has completed, {@code recordMatch} returns its
     * cached result too — so the recorded {@code matchedKey} is byte-for-byte stable across replay.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        // The instance's own correlation key, carried on the start event payload. This is THE key its waitForEvent
        // associates on — never derived from runtime values, so the association is a pure function of the history.
        String ownKey = String.valueOf(ctx.workflowPayload().get("key"));

        ctx.awaitExecute(
                STEP_SETUP,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_SETUP);
                    return Map.of(KEY_OWN_KEY, ownKey);
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));

        // Suspend until a CorrelatedSignalEvent whose `key` equals THIS instance's own key arrives. The association is
        // the correlation surface INV-15 covers: an event for a DIFFERENT key must NOT wake this instance (no
        // cross-wakeup), and the engine's step-name-keyed dedup + terminal-state guards must keep a duplicate of the
        // already-matched key from completing the wait a second time. Mirrors OrderWorkflow.awaitConfirmation exactly,
        // associating on `key` instead of `orderId`.
        CorrelatedSignalEvent signal = ctx.awaitEvent(
                STEP_AWAIT_SIGNAL,
                CorrelatedSignalEvent.class,
                associate(payloadProperty("key"), equalsTo(ownKey)),
                step -> step.timeout(Duration.ofDays(365)));

        // Record the MATCHED event's key — the key of the event that actually woke this instance's wait. In a sound run
        // this is the instance's own key; a foreign key here would mean an event for another instance woke this one (the
        // §10 anti-pattern: an associated event must NOT wake every waiter). Merged into the payload so the engine's
        // reconstructed payload (the history read-model) records it for INV-15 to cross-check against ownKey.
        ctx.awaitExecute(
                STEP_RECORD_MATCH,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_RECORD_MATCH);
                    return Map.of(KEY_MATCHED_KEY, signal.key());
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));
    }
}
