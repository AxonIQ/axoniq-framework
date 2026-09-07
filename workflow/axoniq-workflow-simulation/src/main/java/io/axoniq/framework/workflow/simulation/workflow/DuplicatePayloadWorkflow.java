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
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.DuplicatePayloadSignalEvent;

import java.time.Duration;
import java.util.HashMap;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * A workflow whose body runs a {@code modifyPayload} step that is <strong>not</strong> the last step, then suspends on a
 * never-arriving {@code waitForEvent} — used to settle the S-1 candidate and exercise INVARIANTS.md INV-2
 * ({@code AtMostOnceRecording}) at the {@code modifyPayload}-on-post-crash-live-re-run surface (finding
 * <strong>F-7</strong>).
 * <p>
 * The shape deliberately opens the exact crash window the hypothesis needs: a {@code modifyPayload} step records its
 * {@code <step>:COMPLETED} and that record durably commits, and then — because the next step is a never-arriving wait —
 * the instance stays <strong>LIVE / non-terminal</strong> with no {@code <workflow>:COMPLETED} ever committed. A crash +
 * recover (the harness's ordinary {@link io.axoniq.framework.workflow.simulation.harness.SimulationWorld#crashAndRecover()} seam)
 * then replays the committed {@code <step>:COMPLETED} (so the step is present in the recovered state), switches to live,
 * and re-runs the body for the still-non-terminal instance — re-reaching the {@code modifyPayload} call with the step
 * already present.
 * <p>
 * This is the structural twin of {@link DriftWorkflow#executeV1} (a recorded-terminal step followed by a never-arriving
 * wait that keeps the instance LIVE), but the recorded-terminal step is a {@code modifyPayload}
 * ({@code PayloadDelegate#modifyPayload}) rather than an {@code execute} ({@code ExecuteDelegate}). That single
 * difference is what F-7 turns on: {@code ExecuteDelegate} gates its STARTED/COMPLETED publish on
 * {@code !state().containsStep(stepName)} ({@code ExecuteDelegate.java:117}) and routes its COMPLETED through the
 * guarded {@code AbstractStepExecutor.sendStepEvent} (which refuses to publish an already-terminal step,
 * {@code AbstractStepExecutor.java:204-212}), so a post-crash live re-run <em>skips</em> the re-publish — whereas
 * {@code PayloadDelegate.modifyPayload} gates <em>only</em> the drift guard on {@code !containsStep}
 * ({@code PayloadDelegate.java:80}), appends its publish task <strong>unconditionally</strong>
 * ({@code PayloadDelegate.java:84}) and publishes the {@code COMPLETED} step event <strong>directly</strong> via
 * {@code eventSink.publish(ctx, payloadEvent)} ({@code PayloadDelegate.java:97}) — NOT through {@code sendStepEvent} —
 * so the terminal-step guard never runs and a <strong>second</strong> {@code <step>:COMPLETED} for one step name is
 * re-published to the durable log on the re-run. That second terminal record is the INV-2
 * ({@code AtMostOnceRecording}) violation.
 * <p>
 * Determinism (axon-flow-workflow skill §3.3): the body holds no wall-clock/random/external state — the payload
 * modification is a constant key derived only from the step name — so the crash window and the re-run are fully
 * determined by the (seeded) harness; the signal event is deliberately never delivered, so the wait never resolves.
 * The single {@code modifyPayload} step is the whole point — there is no other recorded-terminal step to confound the
 * per-{@code (workflowId, stepName)} terminal-record count the scenario settles on.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class DuplicatePayloadWorkflow {

    /**
     * Logical workflow name (single definition), stable and distinct from the other simulation workflows.
     */
    public static final String WORKFLOW_NAME = "DuplicatePayloadWorkflow";

    /**
     * The {@code modifyPayload} step whose {@code <step>:COMPLETED} record is committed before the crash and (per F-7)
     * re-published on the post-crash live re-run — the step the scenario counts terminal records for.
     */
    public static final String STEP_FINALIZE_PAYLOAD = "finalizePayload";

    /**
     * The never-arriving {@code waitForEvent} step that keeps the instance LIVE / non-terminal after
     * {@link #STEP_FINALIZE_PAYLOAD} is recorded terminal, so a recovered engine re-runs the body (rather than
     * short-circuiting an already-terminal instance) and re-reaches {@code modifyPayload}.
     */
    public static final String STEP_AWAIT_SIGNAL = "awaitSignal";

    /**
     * The distinct payload key the {@code modifyPayload} step adds (a constant, so the body is deterministic).
     */
    public static final String KEY_FINALIZED = "finalized";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry (recorded into for parity with the other workflows; F-7
     * itself reads only the committed terminal-record count, never the effect counters).
     *
     * @param effects registry that survives crashes.
     */
    public DuplicatePayloadWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The body: a single {@code modifyPayload} step (adds {@link #KEY_FINALIZED}, preserving any prior payload), then a
     * never-arriving {@code waitForEvent} that keeps the instance LIVE / non-terminal — opening the crash window after
     * {@code finalizePayload}'s {@code COMPLETED} commits but before any {@code <workflow>:COMPLETED}.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");

        // modifyPayload (local_only replace): preserve the current payload and add one constant key. Its <step>:COMPLETED
        // commits durably; on a post-crash live re-run this call is re-reached with the step already present (F-7).
        ctx.awaitModifyPayload(
                STEP_FINALIZE_PAYLOAD,
                currentPayload -> {
                    effects.record(workflowId, STEP_FINALIZE_PAYLOAD);
                    var merged = new HashMap<String, Object>(currentPayload);
                    merged.put(KEY_FINALIZED, true);
                    return merged;
                });

        // Never-arriving wait: keeps the instance LIVE / non-terminal (finalizePayload is durably COMPLETED), so a
        // recovered engine running the body re-executes from the start rather than short-circuiting a terminal instance.
        // The signal for this orderId is deliberately never delivered.
        ctx.awaitEvent(
                STEP_AWAIT_SIGNAL,
                DuplicatePayloadSignalEvent.class,
                associate(payloadProperty("orderId"), equalsTo(orderId)),
                step -> step.timeout(Duration.ofDays(365)));
    }
}
