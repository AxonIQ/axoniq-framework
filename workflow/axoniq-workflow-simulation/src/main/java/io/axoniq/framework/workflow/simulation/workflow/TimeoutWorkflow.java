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
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PaymentConfirmedEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * Minimal workflow whose {@code waitForEvent} step <strong>always times out</strong> — used to exercise INVARIANTS.md
 * INV-9 ({@code TimeoutsFire}): a step that exceeds its configured timeout reaches a {@code TIMED_OUT} outcome (recorded)
 * — a timeout never silently hangs or vanishes; the workflow always gets a terminal step outcome it can act on.
 * <p>
 * The timeout is driven the clean, fully-virtual way (the path the task prefers): the wait timeout flows through the
 * injectable {@code WorkflowScheduler}, which the harness wires to {@code ManualWorkflowScheduler} (virtual time). The
 * awaited {@link PaymentConfirmedEvent} is <strong>never delivered</strong>, so once virtual time is advanced past
 * {@link #AWAIT_TIMEOUT} — by the simulator's settle nudges or the {@code CLOCK_JUMP} fault — {@code WaitForDelegate}'s
 * scheduled timeout continuation fires and records the step's {@code TIMED_OUT} event
 * ({@code WaitForDelegate.java} {@code scheduler.delayedExecutor(remainingTimeout)} &rarr; {@code timedOut(stepName)}).
 * <p>
 * Shape:
 * <ol>
 *   <li>{@code reserveInventory} ({@code execute}) — one counting side effect, so there is a real step record
 *       <em>before</em> the timing-out step (otherwise the "timeout produced a terminal outcome" property would have no
 *       earlier history to contrast with);</li>
 *   <li>{@code awaitConfirmation} ({@code awaitEvent} with a short {@link #AWAIT_TIMEOUT} and an {@code orderId}
 *       association) — the event never arrives, so the step times out and the blocking {@code awaitEvent} surfaces the
 *       failure; the body catches it and calls {@code ctx.fail(...)} so the workflow reaches a terminal status (mirrors
 *       {@code examples/simple}'s {@code AwaitEventTimeoutWorkflow}). The engine's {@code default} exception branch does
 *       <strong>not</strong> drive the workflow terminal on a bare {@code RuntimeException}
 *       ({@code SimpleWorkflowExecution.handleWorkflowException}), so catching + {@code ctx.fail} is required for the
 *       instance to terminate — the {@code TIMED_OUT} <em>step</em> record (what INV-9 asserts) is written either way.</li>
 * </ol>
 * The {@code orderId} payload key is deliberately the same shape as {@link OrderWorkflow}'s so the harness can reuse the
 * {@link CountingEffects} key shape and the {@link PaymentConfirmedEvent} correlation; the workflow name and the
 * timing-out step name ({@link #STEP_AWAIT_TIMEOUT}) differ so the two definitions never collide and the per-step
 * timeout bound is unambiguous.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class TimeoutWorkflow {

    /**
     * Logical workflow name, stable and distinct from {@link OrderWorkflow#WORKFLOW_NAME},
     * {@link CancellingWorkflow#WORKFLOW_NAME} and {@link RetryingWorkflow#WORKFLOW_NAME}.
     */
    public static final String WORKFLOW_NAME = "TimeoutWorkflow";

    /**
     * A step that always succeeds, recorded before the timing-out step so there is a non-timing-out record in the
     * history.
     */
    public static final String STEP_RESERVE_INVENTORY = "reserveInventory";

    /**
     * The {@code waitForEvent} step that always times out — the one INV-9 ({@code TimeoutsFire}) checks records a
     * {@code TIMED_OUT} outcome once its timeout window elapses. Deliberately a <strong>distinct</strong> step name from
     * {@link OrderWorkflow#STEP_AWAIT_CONFIRMATION} so the harness's per-step timeout bound applies only to this
     * short-timeout step and never to OrderWorkflow's effectively-infinite (365-day) wait, which shares the same engine
     * in the fuzz.
     */
    public static final String STEP_AWAIT_TIMEOUT = "awaitTimeout";

    /**
     * The wait timeout configured on {@link #STEP_AWAIT_TIMEOUT}. Short and finite (the deliberate contrast with
     * {@link OrderWorkflow}'s effectively-infinite 365-day wait) so the harness can advance virtual time past it and
     * observe the {@code TIMED_OUT} record. Exposed so the harness can assert INV-9 against the configured window without
     * hard-coding the number: once virtual time is at least {@code STARTED + AWAIT_TIMEOUT}, the step must be terminal.
     */
    public static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(5);

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; the {@code execute} body bumps a counter here.
     */
    public TimeoutWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Workflow body: record one successful step, then wait for an event that never arrives so the wait times out.
     * Re-run from the start on every (re)execution; on replay the recorded {@code reserveInventory} returns its cached
     * result and a recorded terminal {@code awaitConfirmation} (TIMED_OUT) replays as a cached result — nothing new is
     * appended.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");

        ctx.awaitExecute(
                STEP_RESERVE_INVENTORY,
                Map.of(),
                (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE_INVENTORY))
        );

        try {
            // Wait for a PaymentConfirmedEvent that the harness never delivers. The wait timeout is scheduled on the
            // injectable WorkflowScheduler (ManualWorkflowScheduler / virtual time); advancing virtual time past
            // AWAIT_TIMEOUT fires the scheduled continuation, which records the step's TIMED_OUT event and surfaces the
            // failure here. INV-9 (TimeoutsFire) asserts that TIMED_OUT record exists once the window has elapsed.
            ctx.awaitEvent(
                    STEP_AWAIT_TIMEOUT,
                    PaymentConfirmedEvent.class,
                    associate(payloadProperty("orderId"), equalsTo(orderId)),
                    step -> step.timeout(AWAIT_TIMEOUT)
            );
        } catch (RuntimeException timedOut) {
            // The blocking awaitEvent surfaces the timed-out step as a failure (the step is recorded TIMED_OUT). The
            // engine does not drive the workflow terminal on a bare RuntimeException, so terminate explicitly with
            // ctx.fail so the instance reaches a terminal status (the step record stays TIMED_OUT regardless).
            ctx.fail(timedOut);
        }
    }
}
