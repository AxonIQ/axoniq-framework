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
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.DefaultTimeoutFutureResolver;
import io.axoniq.framework.workflow.runtime.util.FutureResolver;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.LogCapture;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.PayloadOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.PublishChainWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PayloadOrderRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishChainRequestedEvent;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.List;

import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.isTerminal;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.publishRecords;
import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.stepNamesById;

/**
 * The publish append when the event store misbehaves in the two ways the fence and the vanish do not cover.
 * <ul>
 *   <li>{@link #appendStallsPastResolutionTimeout}: the commit never answers. The publisher's {@code FutureResolver}
 *   gives up after the resolution timeout (shortened to 2 s here through the component registry), the engine logs it
 *   and stops the runtime for recovery, nothing is durable. A restart publishes once and the chain completes.</li>
 *   <li>{@link #appendFails}: the commit completes exceptionally with a plain runtime exception. What the engine does
 *   with the publisher <em>before</em> any restart is observed and reported, not assumed; a restart must then publish
 *   once and the chain must complete.</li>
 *   <li>{@link #modifyPayloadAppendFails}: the same failed commit on a {@code modifyPayload} step of another workflow,
 *   the contrast that says whether the pre-restart behaviour is publish-specific or engine-wide.</li>
 * </ul>
 * ORACLE: the store's own stall/fail counters (landing evidence), the engine's log line for the resolution timeout, the
 * publish record count by distinct identifier, terminal statuses, live set. WORKLOAD: the three chain definitions, one
 * order id, one armed store fault, one restart. AMBIGUITY: the pre-restart state after a failed commit is reported as
 * observed; only the post-restart record count and completion are asserted as properties. BUDGET: one seed, ~10 s per
 * probe.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class PublishAppendFailureScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(15);
    private static final Duration OBSERVATION_WINDOW = Duration.ofSeconds(3);
    private static final Duration RESOLUTION_TIMEOUT = Duration.ofSeconds(2);
    private static final String EXECUTION_LOGGER =
            "io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowExecution";

    private PublishAppendFailureScenario() {
    }

    /**
     * Outcome of a store-fault probe.
     *
     * @param faultLanded          the armed stall or failure consumed a commit (landing evidence).
     * @param recordsBeforeRestart distinct publish-step records while the fault stands (expected 0).
     * @param timeoutLogged        the engine logged the publication resolution timeout (stall probe only).
     * @param publisherStatus      the publisher's workflow status in the log before the restart ({@code null} if none
     *                             terminal): what the engine did with the failed append.
     * @param publisherLive        the publisher was still in the live set before the restart.
     * @param recordsAfterRestart  distinct publish-step records after the restart (expected 1).
     * @param allTerminal          the whole chain reached a terminal status after the restart.
     */
    public record Outcome(boolean faultLanded, int recordsBeforeRestart, boolean timeoutLogged,
                          @Nullable WorkflowStatus publisherStatus, boolean publisherLive,
                          int recordsAfterRestart, boolean allTerminal) {
    }

    /**
     * The publish commit hangs until the resolution timeout.
     *
     * @param seed    world seed.
     * @param orderId order id of the chain.
     * @return the outcome.
     */
    public static Outcome appendStallsPastResolutionTimeout(long seed, String orderId) {
        var effects = new CountingEffects();
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        var log = LogCapture.attach(EXECUTION_LOGGER);
        try (var world = SimulationWorld.withExtraRegistrations(
                seed, EngineInstance.publishChainWorkflow(effects),
                registry -> registry.registerComponent(FutureResolver.class,
                                                       cfg -> new DefaultTimeoutFutureResolver(RESOLUTION_TIMEOUT)))) {
            world.eventStore().armStallCommitFor(PublishChainWorkflow.STEP_PUBLISH_REQUEST, StepStatus.COMPLETED);
            world.engine().publish(new PublishChainRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the stall to consume the publish commit",
                                () -> world.eventStore().stalledCommits() >= 1);
            boolean timeoutLogged = Polling.await(DEADLINE.plus(RESOLUTION_TIMEOUT),
                                                  () -> timeoutLogged(log, requester));
            var before = observeBeforeRestart(world, requester, PublishChainWorkflow.STEP_PUBLISH_REQUEST);
            world.crashAndRecover();
            return finishChain(world, orderId, before.records(), timeoutLogged, before.status(), before.live());
        } finally {
            log.close();
        }
    }

    /**
     * The publish commit fails with a plain runtime exception.
     *
     * @param seed    world seed.
     * @param orderId order id of the chain.
     * @return the outcome.
     */
    public static Outcome appendFails(long seed, String orderId) {
        var effects = new CountingEffects();
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        try (var world = new SimulationWorld(seed, EngineInstance.publishChainWorkflow(effects))) {
            world.eventStore().armFailCommitFor(PublishChainWorkflow.STEP_PUBLISH_REQUEST, StepStatus.COMPLETED);
            world.engine().publish(new PublishChainRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the failure to consume the publish commit",
                                () -> world.eventStore().failedCommits() >= 1);
            // Give the engine a bounded window to react to the failed append, then look, not assume.
            Polling.await(OBSERVATION_WINDOW, () -> isTerminal(world.committedLog(), requester));
            var before = observeBeforeRestart(world, requester, PublishChainWorkflow.STEP_PUBLISH_REQUEST);
            world.crashAndRecover();
            return finishChain(world, orderId, before.records(), false, before.status(), before.live());
        }
    }

    /**
     * Contrast: the same failed commit on the {@code finalizeOrder} step of {@link PayloadOrderWorkflow}.
     *
     * @param seed    world seed.
     * @param orderId order id of the payload workflow.
     * @return the outcome, with {@code recordsBeforeRestart} / {@code recordsAfterRestart} counting the
     * {@code finalizeOrder} COMPLETED records.
     */
    public static Outcome modifyPayloadAppendFails(long seed, String orderId) {
        var effects = new CountingEffects();
        String workflowId = "payload-" + orderId;
        try (var world = new SimulationWorld(seed, EngineInstance.payloadOrderWorkflow(effects))) {
            world.eventStore().armFailCommitFor(PayloadOrderWorkflow.STEP_FINALIZE_ORDER, StepStatus.COMPLETED);
            world.engine().publish(new PayloadOrderRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the failure to consume the modifyPayload commit",
                                () -> world.eventStore().failedCommits() >= 1);
            Polling.await(OBSERVATION_WINDOW, () -> isTerminal(world.committedLog(), workflowId));
            var before = observeBeforeRestart(world, workflowId, PayloadOrderWorkflow.STEP_FINALIZE_ORDER);
            world.crashAndRecover();
            boolean settled = Polling.await(DEADLINE, () -> isTerminal(world.committedLog(), workflowId));
            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new Outcome(true, before.records(), false, before.status(), before.live(),
                               publishRecords(world.committedLog(), workflowId, PayloadOrderWorkflow.STEP_FINALIZE_ORDER),
                               settled);
        }
    }

    private record Before(int records, @Nullable WorkflowStatus status, boolean live) {
    }

    private static Before observeBeforeRestart(SimulationWorld world, String workflowId, String stepName) {
        var log = world.committedLog();
        var status = log.stream()
                        .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                        .map(e -> MetadataUtils.getWorkflowStatus(e.metadata()).orElse(null))
                        .filter(s -> s != null && s.isTerminal())
                        .findFirst()
                        .orElse(null);
        return new Before(publishRecords(log, workflowId, stepName), status,
                          world.engine().liveWorkflowIds().contains(workflowId));
    }

    private static Outcome finishChain(SimulationWorld world, String orderId, int recordsBefore,
                                       boolean timeoutLogged, @Nullable WorkflowStatus status, boolean live) {
        String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
        boolean settled = Polling.await(DEADLINE, () -> allTerminal(world, orderId));
        if (!settled && !isTerminal(world.committedLog(), requester)) {
            // Same rescue as PublishCrashScenario: a reply that landed in a crash window is re-delivered once.
            world.engine().publish(new io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishReplyEvent(orderId));
            settled = Polling.await(DEADLINE, () -> allTerminal(world, orderId));
        }
        Invariants.assertAtMostOnceRecording(world.committedLog());
        Invariants.assertOneInstancePerStart(world.committedLog());
        Invariants.assertNoForeignStepRecorded(stepNamesById(world));
        return new Outcome(true, recordsBefore, timeoutLogged, status, live,
                           publishRecords(world.committedLog(), requester, PublishChainWorkflow.STEP_PUBLISH_REQUEST),
                           settled);
    }

    private static boolean allTerminal(SimulationWorld world, String orderId) {
        var log = world.committedLog();
        return List.of(PublishChainWorkflow.REQUESTER_ID_PREFIX, PublishChainWorkflow.RESPONDER_ID_PREFIX,
                       PublishChainWorkflow.OBSERVER_ID_PREFIX)
                   .stream().allMatch(prefix -> isTerminal(log, prefix + orderId));
    }

    private static boolean timeoutLogged(LogCapture log, String workflowId) {
        return log.messages().stream().anyMatch(m -> m.contains("resolution timeout") && m.contains(workflowId));
    }
}
