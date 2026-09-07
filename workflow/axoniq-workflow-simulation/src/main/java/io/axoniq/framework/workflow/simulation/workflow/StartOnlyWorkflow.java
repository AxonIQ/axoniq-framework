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
 * Minimal workflow that <strong>starts and stays LIVE</strong> (it records one step and then suspends on a
 * {@code waitForEvent} whose event is not delivered during the scenario window), used to exercise INVARIANTS.md INV-10
 * ({@code OneInstancePerStart}) — that a single start for a business key yields exactly one live instance, and a
 * duplicate/redelivered start while the instance is LIVE does not create a second concurrent instance.
 * <p>
 * The two terminal test workflows ({@link CancellingWorkflow} reaches CANCELLED, {@link OrderWorkflow} reaches
 * COMPLETED) are unsuitable for the live-dedup case: the engine's spawn-dedup only rejects a duplicate start while the
 * instance is still <em>live</em> in the in-memory repository (see {@code WorkflowSpawnRouting.resolveWorkflowIdForNewSpawn}).
 * Once an instance terminates it is evicted and a redelivered start re-spawns it — that is the documented finding
 * <strong>F-3</strong>, the after-terminal exception INV-10 deliberately tolerates. To probe the live-dedup property
 * this workflow therefore parks itself in a non-terminal {@code awaitEvent} so the duplicate start arrives while it is
 * live:
 * <ol>
 *   <li>{@code reserveInventory} ({@code execute}) — one counting side effect, so there is a real step record after the
 *       start;</li>
 *   <li>{@code awaitConfirmation} ({@code waitForEvent} with an {@code orderId} association and an effectively-infinite
 *       365-day timeout) — suspends the instance, keeping it LIVE (non-terminal) for the duration of the scenario. The
 *       scenario never delivers the awaited {@link PaymentConfirmedEvent}, so the instance stays parked while the
 *       duplicate start is redelivered.</li>
 * </ol>
 * The step names deliberately reuse {@code OrderWorkflow}'s strings so the harness can reuse the {@link CountingEffects}
 * key shape; the workflow name differs so the definitions never collide. Wired through the same
 * {@code EngineInstance.WorkflowRegistration} mechanism INV-7 generalized, so it reuses all the infrastructure.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class StartOnlyWorkflow {

    /**
     * Logical workflow name, stable and distinct from the other simulation workflows.
     */
    public static final String WORKFLOW_NAME = "StartOnlyWorkflow";

    /**
     * The single step that runs (a counting side effect) before the workflow parks on its wait.
     */
    public static final String STEP_RESERVE_INVENTORY = "reserveInventory";

    /**
     * The wait step that keeps the instance LIVE (non-terminal) so a duplicate start arrives while it is live.
     */
    public static final String STEP_AWAIT_CONFIRMATION = "awaitConfirmation";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; the single {@code execute} body bumps a counter here.
     */
    public StartOnlyWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Workflow body: run one recorded step, then suspend on a never-arriving wait so the instance stays LIVE. Re-run
     * from the start on every (re)execution; on replay the recorded {@code reserveInventory} returns its cached result
     * and the body re-suspends on the same wait.
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

        // Park the instance in a non-terminal wait so it stays LIVE while the scenario redelivers the start event. The
        // awaited PaymentConfirmedEvent is never delivered during the scenario, and the 365-day timeout keeps the wait
        // from firing on the small virtual-time nudges the harness makes — so the instance remains live and the engine's
        // spawn-dedup is exercised on a genuinely-live instance (INV-10).
        ctx.awaitEvent(
                STEP_AWAIT_CONFIRMATION,
                PaymentConfirmedEvent.class,
                associate(payloadProperty("orderId"), equalsTo(orderId)),
                step -> step.timeout(Duration.ofDays(365))
        );
    }
}
