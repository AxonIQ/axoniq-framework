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

import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.OrderPlacedEvent;
import org.jspecify.annotations.Nullable;

import java.time.Duration;

/**
 * Bridges TLA+ {@code MC_effect.cfg} / {@code EffectAtMostOnce} (INVARIANTS.md INV-6, finding F-0) — now the
 * <strong>fixed</strong> behaviour. Hand-authored, fully deterministic scenario that drives the real engine through the
 * write-then-vanish crash window and confirms the engine's at-most-once guarantee.
 * <p>
 * The implemented fix corresponds to TLA+ {@code MC_effect_fixed.cfg} ({@code APPEND_CONDITION=TRUE}): a step left
 * {@code STARTED} on a post-crash resume is <strong>not</strong> re-executed, so {@code effect[s]} never reaches 2. The
 * engine realises this via skip-and-resolve through the regular error flow — the interrupted attempt becomes a
 * {@code StepIndeterminateException} failure ({@code chargePayment} has no retry policy, so it resolves to
 * {@code FAILED}).
 * <p>
 * Steps:
 * <ol>
 *   <li>arm the durable store to drop the {@code chargePayment} {@code COMPLETED} commit;</li>
 *   <li>publish the start event — the body runs {@code reserveInventory} (effect #1) and the {@code chargePayment}
 *       action (effect #1), then attempts to commit {@code chargePayment} {@code COMPLETED}, which vanishes. The step is
 *       left {@code STARTED} in the durable log (the engine never sees the COMPLETED back), so the instance is parked
 *       mid-flight;</li>
 *   <li>crash + recover: replay rebuilds {@code chargePayment} as {@code STARTED}, switches to live, and re-runs the
 *       body — {@code chargePayment} is found {@code STARTED} <em>at the execute entry</em>, so the engine does
 *       <strong>not</strong> re-run the action (effect stays at 1) and resolves the step to {@code FAILED}. The
 *       {@code OrderWorkflow} body then surfaces that failure and terminates via {@code ctx.fail}.</li>
 * </ol>
 * The harness asserts the effect ran <strong>exactly once</strong> (at-most-once holds), that the step resolved to a
 * single terminal {@code FAILED} record, and that at-most-once <em>recording</em> still holds (INV-2).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class WriteThenVanishScenario {

    private WriteThenVanishScenario() {
    }

    /**
     * Result of running the scenario, for tests and the Phase-5 hand-off.
     *
     * @param chargeEffectCount         how many times the {@code chargePayment} action ran (1 = at-most-once holds; the
     *                                  interrupted attempt was not re-run).
     * @param chargeTerminalRecordCount how many terminal {@code chargePayment} records are in the committed log.
     * @param chargeTerminalStatus      the terminal status {@code chargePayment} resolved to ({@code FAILED} under the
     *                                  at-most-once fix), or {@code null} if it has no terminal record yet.
     * @param chargeFailureCauseType    the fully-qualified type recorded as the {@code chargePayment} FAILED step's
     *                                  cause ({@code StepIndeterminateException} under the at-most-once fix — the
     *                                  durable proof that the not-re-run attempt surfaced the dedicated cause), or
     *                                  {@code null} if there is no FAILED record.
     */
    public record Outcome(int chargeEffectCount, int chargeTerminalRecordCount,
                          @Nullable StepStatus chargeTerminalStatus, @Nullable String chargeFailureCauseType) {

    }

    /**
     * Runs the scenario against a fresh world seeded with {@code seed} and returns what it observed.
     *
     * @param seed     seed for the world's deterministic id source.
     * @param orderId  business key for the single order instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        try (var world = new SimulationWorld(seed)) {
            String workflowId = "order-" + orderId;

            // 1. Arm the F-0 window: the chargePayment COMPLETED commit will vanish.
            world.eventStore().armVanishCommitFor(OrderWorkflow.STEP_CHARGE_PAYMENT, StepStatus.COMPLETED);

            // 2. Start the workflow. reserveInventory + the chargePayment action run; COMPLETED commit vanishes.
            world.engine().publish(new OrderPlacedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "chargePayment effect to run once",
                                () -> world.effects().count(workflowId, OrderWorkflow.STEP_CHARGE_PAYMENT) >= 1);
            // The vanish must have fired (COMPLETED was dropped), so chargePayment is STARTED-only in the durable log.
            Polling.awaitOrFail(Duration.ofSeconds(10), "chargePayment COMPLETED commit to vanish",
                                () -> !world.eventStore().isVanishArmed());

            // 3. Crash + recover: AT-MOST-ONCE — replay finds chargePayment STARTED and does NOT re-run the action;
            //    it resolves the interrupted attempt to a terminal FAILED record instead.
            world.crashAndRecover();
            Polling.awaitOrFail(Duration.ofSeconds(10),
                                "chargePayment to resolve to a terminal record after recovery (not re-run)",
                                () -> chargeTerminalStatus(world, workflowId) != null);

            int chargeEffectCount = world.effects().count(workflowId, OrderWorkflow.STEP_CHARGE_PAYMENT);

            // At-most-once EFFECT (INV-6 / F-0, the fix): the action must NOT have re-run on recovery.
            Invariants.assertEffectAtMostOnce(workflowId, OrderWorkflow.STEP_CHARGE_PAYMENT, chargeEffectCount);
            // At-most-once RECORDING (INV-2) must still hold.
            Invariants.assertAtMostOnceRecording(world.committedLog());

            return new Outcome(chargeEffectCount, chargeTerminalRecordCount(world, workflowId),
                               chargeTerminalStatus(world, workflowId), chargeFailureCauseType(world, workflowId));
        }
    }

    /**
     * The fully-qualified cause type recorded on the {@code chargePayment} FAILED step (the FAILED step event's payload
     * is a {@link WorkflowError} whose {@code type} is the cause's class name), or {@code null} if none. Under the
     * at-most-once fix this is {@code StepIndeterminateException} — the durable evidence that re-reaching a
     * previously-{@code STARTED} step did not re-run the action but surfaced the dedicated indeterminate cause.
     */
    @Nullable
    private static String chargeFailureCauseType(SimulationWorld world, String workflowId) {
        return world.committedLog().stream()
                .filter(e -> OrderWorkflow.STEP_CHARGE_PAYMENT.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(StepStatus.FAILED::equals).orElse(false))
                .map(e -> e.payload() instanceof WorkflowError we ? we.type() : null)
                .filter(java.util.Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    /**
     * The terminal status of {@code chargePayment} in the committed log, or {@code null} if it has none yet.
     */
    @Nullable
    private static StepStatus chargeTerminalStatus(SimulationWorld world, String workflowId) {
        return world.committedLog().stream()
                .filter(e -> OrderWorkflow.STEP_CHARGE_PAYMENT.equals(MetadataUtils.getStepName(e.metadata())))
                .map(e -> MetadataUtils.getStepStatus(e.metadata()).orElse(null))
                .filter(s -> s != null && s.isTerminal())
                .findFirst()
                .orElse(null);
    }

    /**
     * How many terminal {@code chargePayment} records are present in the committed log.
     */
    private static int chargeTerminalRecordCount(SimulationWorld world, String workflowId) {
        return (int) world.committedLog().stream()
                .filter(e -> OrderWorkflow.STEP_CHARGE_PAYMENT.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(StepStatus::isTerminal).orElse(false))
                .count();
    }
}
