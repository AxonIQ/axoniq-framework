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
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.PublishChainWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishChainRequestedEvent;

import java.time.Duration;

/**
 * A stale writer's publish is fenced like every other engine append: a foreign write landing between the DCB
 * condition checks of the publish's commit gets the append rejected, so no published event exists, the responder and
 * observer it would have started never start, and the fenced publisher records nothing terminal.
 * <p>
 * ORACLE: records before and after the fence, the publish record count, responder starts, terminal records, and the
 * rejection logged by the execution. WORKLOAD: the three chain definitions, one order id, one armed fence. EVIDENCE:
 * {@code isFenceArmed()} flips when the fence consumed the commit; the rejection is read from the engine's log.
 * AMBIGUITY: none, every observable is a count. BUDGET: one seed, ~20 s.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FencedPublishScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(20);
    private static final Duration REJECTION_WINDOW = Duration.ofSeconds(15);

    private FencedPublishScenario() {
    }

    /**
     * What the fenced publish left behind.
     *
     * @param recordsBeforeFence  the publisher's committed records when the fence consumed the publish commit.
     * @param recordsAfterFence   the publisher's committed records after the rejection window.
     * @param publishRecords      distinct records of the publish step (expected 0).
     * @param responderStarts     workflow STARTED records of the responder (expected 0: nothing was published).
     * @param terminalRecords     the publisher's terminal workflow-status records (expected 0).
     * @param rejections          append rejections logged for the publisher (expected at least 1).
     */
    public record Outcome(int recordsBeforeFence, int recordsAfterFence, int publishRecords, int responderStarts,
                          int terminalRecords, int rejections) {
    }

    /**
     * Runs the fenced publish.
     *
     * @param seed    world seed.
     * @param orderId order id shared by the chain.
     * @return the outcome.
     */
    public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        String responder = PublishChainWorkflow.RESPONDER_ID_PREFIX + orderId;
        var appender = FenceOracles.attachRejectionAppender();
        try (var world = new SimulationWorld(seed, EngineInstance.publishChainWorkflow(effects))) {
            world.eventStore().armForeignWriteBeforeCommitOf(PublishChainWorkflow.STEP_PUBLISH_REQUEST,
                                                             StepStatus.COMPLETED);
            world.engine().publish(new PublishChainRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the fence to consume the publish commit",
                                () -> !world.eventStore().isFenceArmed());
            int before = FenceOracles.records(world.committedLog(), requester);
            Polling.await(REJECTION_WINDOW, () -> FenceOracles.rejections(appender, requester) >= 1);
            return new Outcome(before,
                               FenceOracles.records(world.committedLog(), requester),
                               PublishOracles.publishRecords(world.committedLog(), requester,
                                                             PublishChainWorkflow.STEP_PUBLISH_REQUEST),
                               PublishOracles.starts(world.committedLog(), responder),
                               FenceOracles.terminalRecords(world.committedLog(), requester),
                               FenceOracles.rejections(appender, requester));
        } finally {
            FenceOracles.detach(appender);
        }
    }
}
