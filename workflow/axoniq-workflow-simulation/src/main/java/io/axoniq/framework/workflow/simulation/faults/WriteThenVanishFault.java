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
package io.axoniq.framework.workflow.simulation.faults;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationContext;
import io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow;

import java.time.Duration;

/**
 * Write-then-vanish: a crash in the window <em>after</em> an {@code execute} action runs but <em>before</em> its
 * {@code COMPLETED} event commits — the F-0 window (INVARIANTS.md INV-6).
 * <p>
 * It arms the durable store to drop the {@code chargePayment} {@code COMPLETED} commit specifically (the step's
 * effect has already run and been counted by then, since the action runs after the {@code STARTED} commit — see
 * {@code ExecuteDelegate}). The next time the engine pumps that step the COMPLETED append vanishes, leaving the step
 * {@code STARTED} in the durable log. A crash + recovery then finds the step still {@code STARTED} and re-runs the
 * action on replay, so the effect counter reaches 2. This is the implementation-level confirmation of the TLA+
 * {@code EffectAtMostOnce} counterexample; the harness records it as a documented expected violation rather than a
 * build break.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class WriteThenVanishFault implements Fault {

        @Override
    public FaultKind kind() {
        return FaultKind.WRITE_THEN_VANISH;
    }

    @Override
    public void apply(SimulationContext context) {
        // Only arm if chargePayment has not yet durably completed; otherwise the window has passed for this run.
        boolean chargeAlreadyCompleted = context.world().committedLog().stream()
                .anyMatch(e -> MetadataUtils.getStepStatus(e.metadata()).map(StepStatus::isTerminal).orElse(false)
                        && OrderWorkflow.STEP_CHARGE_PAYMENT.equals(MetadataUtils.getStepName(e.metadata())));
        if (chargeAlreadyCompleted) {
            context.record("WRITE_THEN_VANISH: chargePayment already committed COMPLETED — window passed, no-op");
            return;
        }
        // Arm the F-0 window: the chargePayment COMPLETED commit will vanish. The effect runs before that commit, so
        // dropping it leaves the step STARTED in the durable log with the effect counted.
        var eventStore = context.world().eventStore();
        eventStore.armVanishCommitFor(OrderWorkflow.STEP_CHARGE_PAYMENT, StepStatus.COMPLETED);
        // Give the body a brief window to attempt (and vanish) the COMPLETED commit.
        Polling.await(Duration.ofSeconds(2), () -> !eventStore.isVanishArmed());
        // Always follow with a crash + recovery: this is the "vanish" half of write-then-vanish. Recovery rebuilds
        // chargePayment as STARTED and re-runs the action (reproducing F-0 when the vanish fired) and, crucially, lets
        // the instance make progress again — without recovery a vanished COMPLETED would park the workflow forever.
        context.record("WRITE_THEN_VANISH: armed=" + (!eventStore.isVanishArmed() ? "fired" : "not-fired")
                + ", crashing + recovering");
        context.world().crashAndRecover();
    }
}
