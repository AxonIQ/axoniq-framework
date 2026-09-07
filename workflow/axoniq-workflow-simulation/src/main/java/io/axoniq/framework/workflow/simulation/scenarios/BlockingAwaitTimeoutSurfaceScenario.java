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
import io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow.CapturedSurface;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BlockingAwaitExecuteTimeoutRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BlockingTypedAwaitEventTimeoutRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BlockingUntypedAwaitEventTimeoutRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BlockingUntypedAwaitExecuteTimeoutRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * Deterministic scenario settling the S-2 candidate (finding <strong>F-8</strong>): the
 * <strong>blocking-convenience timeout-surfacing asymmetry</strong>. When a step TIMES OUT on a blocking convenience
 * call, the typed {@code awaitEvent(stepName, Class, ...)} overload special-cases {@code result.timeout()} and throws a
 * clean {@code StepTimedOutException}, but the {@code awaitExecute(...)} family (typed and untyped) and the untyped
 * {@code awaitEvent(stepName, EventCondition)} go through {@code AbstractDSLWorkflowContext.resolveStepPayload} and
 * surface {@code StepFailedException} wrapping a {@code null} cause instead.
 * <p>
 * Drives {@link BlockingAwaitTimeoutWorkflow}'s four entry points, each on its own fresh world so a single
 * blocking-convenience timeout is driven in isolation. For each, the body catches the {@link Throwable} the blocking
 * convenience call surfaces and records the observed {@link CapturedSurface} (thrown class + whether its cause was
 * {@code null}); the scenario returns those four surfaces so the test can lock the asymmetry.
 * <p>
 * Two driving techniques, both fully virtual:
 * <ul>
 *   <li><strong>{@code execute}-path timeouts</strong> (typed + untyped {@code awaitExecute}) ride the non-injectable
 *       {@code orTimeout} residual (ARCHITECTURE.md §12 / adoc D5, exactly like INV-9's wait timeout). The scenario
 *       <strong>pre-advances</strong> the lock-step {@code MutableClock} past the step's window BEFORE publishing the
 *       start event (the {@code RetryTimingAndExhaustionEdgesScenario#runTimeoutEdge} technique): the engine computes
 *       {@code remainingTimeout = recordedStartedTimestamp + timeout - Instant.now(clock)}; pre-advancing the
 *       {@code MutableClock} to {@code (system wall time + timeout + buffer)} makes it negative, so {@code ExecuteDelegate}
 *       fires its negative-remaining branch and records {@code TIMED_OUT} on the next task cycle — no wall-clock waiting.
 *       </li>
 *   <li><strong>{@code awaitEvent}-path timeouts</strong> (untyped + typed) ride the injectable scheduler. The scenario
 *       reads the step's recorded STARTED timestamp and advances virtual time to {@code started + timeout + buffer} (the
 *       {@code Inv9TimeoutsFireScenario} technique), firing the scheduled wait-timeout continuation.</li>
 * </ul>
 * Scenario-pinned only (each registration is scenario-only, not in {@link SimulationWorld#defaultRegistrations()}), so
 * the always-on fuzz instance counts are unperturbed and the {@code orTimeout} residual stays out of the per-step fuzz
 * set (Phase-3 D5).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class BlockingAwaitTimeoutSurfaceScenario {

    /**
     * The (default) per-attempt {@code execute} timeout the typed {@code awaitExecute(stepName, Class, Supplier)} step
     * carries (no customizer overload, so it uses {@code BaseWorkflowContext}'s default 5s), and the explicit short
     * {@code timeout} the untyped {@code awaitExecute} / {@code awaitEvent} steps and the wait-timeout pre-advance use.
     */
    public static final Duration STEP_TIMEOUT = Duration.ofSeconds(5);

    private BlockingAwaitTimeoutSurfaceScenario() {
    }

    /**
     * The four observed surfaces, one per blocking-convenience path.
     *
     * @param typedAwaitExecute   surface thrown by the typed {@code awaitExecute(stepName, Class, Supplier)} on timeout
     *                            (F-8 mis-classified: expected {@code StepFailedException} with a {@code null} cause).
     * @param untypedAwaitExecute surface thrown by the untyped {@code awaitExecute(stepName, Map, processor, customizer)}
     *                            on timeout (F-8 mis-classified).
     * @param untypedAwaitEvent   surface thrown by the untyped {@code awaitEvent(stepName, EventCondition)} on timeout
     *                            (F-8 mis-classified).
     * @param typedAwaitEvent     surface thrown by the typed {@code awaitEvent(stepName, Class, conditions, customizer)}
     *                            on timeout — the CONTRAST case (expected {@code StepTimedOutException}).
     */
    public record Outcome(CapturedSurface typedAwaitExecute, CapturedSurface untypedAwaitExecute,
                          CapturedSurface untypedAwaitEvent, CapturedSurface typedAwaitEvent) {

    }

    /**
     * Runs all four blocking-convenience timeout paths (each on its own fresh world) and returns the four observed
     * exception surfaces.
     *
     * @param seed    seed for each world's deterministic id source.
     * @param orderId business key for the single instance in each world.
     * @return the four observed surfaces.
     */
        public static Outcome run(long seed, String orderId) {
        // The execute-path action blocks on this never-released latch so the per-attempt orTimeout window
        // deterministically elapses. CountDownLatch.await() responds to interruption, so engine shutdown can unwind it.
        var execLatch = new CountDownLatch(1);
        var typedExecWorkflow = new BlockingAwaitTimeoutWorkflow(new CountingEffects(), execLatch);
        var untypedExecWorkflow = new BlockingAwaitTimeoutWorkflow(new CountingEffects(), execLatch);
        // The awaitEvent paths never run the execute-path action, so their latch is irrelevant.
        var untypedEventWorkflow = new BlockingAwaitTimeoutWorkflow(new CountingEffects(), new CountDownLatch(1));
        var typedEventWorkflow = new BlockingAwaitTimeoutWorkflow(new CountingEffects(), new CountDownLatch(1));
        return new Outcome(
                runExecutePath(seed, orderId, "awaitexec-", typedExecWorkflow,
                               EngineInstance.blockingAwaitExecuteTimeoutWorkflow(typedExecWorkflow),
                               new BlockingAwaitExecuteTimeoutRequestedEvent(orderId)),
                runExecutePath(seed, orderId, "awaitexecu-", untypedExecWorkflow,
                               EngineInstance.blockingUntypedAwaitExecuteTimeoutWorkflow(untypedExecWorkflow),
                               new BlockingUntypedAwaitExecuteTimeoutRequestedEvent(orderId)),
                runWaitPath(seed, orderId, "awaitevtu-", untypedEventWorkflow,
                            EngineInstance.blockingUntypedAwaitEventTimeoutWorkflow(untypedEventWorkflow),
                            new BlockingUntypedAwaitEventTimeoutRequestedEvent(orderId)),
                runWaitPath(seed, orderId, "awaitevt-", typedEventWorkflow,
                            EngineInstance.blockingTypedAwaitEventTimeoutWorkflow(typedEventWorkflow),
                            new BlockingTypedAwaitEventTimeoutRequestedEvent(orderId)));
    }

    /**
     * Drives one {@code execute}-path entry point: pre-advance the {@code MutableClock} past the step window, publish the
     * start event, let the per-attempt timeout fire the {@code TIMED_OUT} record, and capture the surfaced exception.
     */
        private static CapturedSurface runExecutePath(
            long seed, String orderId, String idPrefix,
            BlockingAwaitTimeoutWorkflow workflow,
            EngineInstance.WorkflowRegistration registration, Object startEvent) {
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = idPrefix + orderId;

            // PRE-ADVANCE the lock-step virtual clock so the blocking step's per-attempt timeout window is ALREADY
            // elapsed by the time the step records STARTED and ExecuteDelegate computes its remainingTimeout (the
            // RetryTimingAndExhaustionEdgesScenario#runTimeoutEdge technique). The recorded STARTED timestamp comes from
            // the in-memory event store's GenericEventMessage clock (the un-injectable static wall clock — D5 residual,
            // ~system time), while the harness MutableClock starts at the Unix epoch; advance to (system wall time +
            // STEP_TIMEOUT + a generous buffer) so remainingTimeout is negative and TIMED_OUT records IMMEDIATELY on the
            // next task cycle (the negative-remaining branch), no wall-clock waiting on the orTimeout JDK timer.
            Duration preAdvance = Duration.between(Instant.EPOCH, Instant.now())
                                          .plus(STEP_TIMEOUT)
                                          .plus(Duration.ofMinutes(5));
            world.advanceTime(preAdvance);

            world.engine().publish(startEvent);

            // The blocking step must reach a terminal (TIMED_OUT) step record, then the workflow must terminate (the
            // body catches the surfaced exception and calls ctx.fail) — never hanging.
            Polling.awaitOrFail(Duration.ofSeconds(30),
                                "the blocking " + idPrefix + " step to reach a terminal (TIMED_OUT) step status",
                                () -> hasTerminalStep(world.committedLog(), workflowId,
                                                      BlockingAwaitTimeoutWorkflow.STEP_BLOCKING));
            Polling.awaitOrFail(Duration.ofSeconds(30),
                                "the blocking convenience call to surface an exception (captured by the body)",
                                () -> workflow.capturedSurface() != null);
            return workflow.capturedSurface();
        }
    }

    /**
     * Drives one {@code awaitEvent}-path entry point: publish the start event, advance virtual time past the step's
     * recorded STARTED + timeout window (the injectable scheduler fires the wait timeout), and capture the surfaced
     * exception.
     */
        private static CapturedSurface runWaitPath(
            long seed, String orderId, String idPrefix,
            BlockingAwaitTimeoutWorkflow workflow,
            EngineInstance.WorkflowRegistration registration, Object startEvent) {
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = idPrefix + orderId;

            world.engine().publish(startEvent);
            // The wait step parks STARTED with a scheduled timeout continuation; wait until it is committed and the
            // timer is queued on the (virtual-time) scheduler before advancing.
            Polling.awaitOrFail(Duration.ofSeconds(10), "the blocking " + idPrefix + " wait step to reach STARTED",
                                () -> hasStep(world.committedLog(), workflowId,
                                              BlockingAwaitTimeoutWorkflow.STEP_BLOCKING));
            Polling.awaitOrFail(Duration.ofSeconds(10), "the wait-timeout to be scheduled on the virtual scheduler",
                                () -> world.scheduler().pendingTasks() > 0);

            // Advance virtual time past the wait timeout — derive the advance from the step's recorded STARTED
            // timestamp (the Inv9TimeoutsFireScenario technique): the engine anchors the wait-timeout deadline on
            // started + STEP_TIMEOUT, and the recorded timestamp and the virtual clock can sit in different eras (the D5
            // static event-store clock vs the epoch-anchored MutableClock), so advance to started + STEP_TIMEOUT + 1s.
            Instant started = startedAt(world.committedLog(), workflowId, BlockingAwaitTimeoutWorkflow.STEP_BLOCKING)
                    .orElseThrow();
            Instant fireBy = started.plus(STEP_TIMEOUT).plusSeconds(1);
            Duration advance = Duration.between(world.clock().instant(), fireBy);
            world.advanceTime(advance.isNegative() ? Duration.ZERO : advance);

            Polling.awaitOrFail(Duration.ofSeconds(30),
                                "the blocking " + idPrefix + " wait step to reach a terminal (TIMED_OUT) step status",
                                () -> hasTerminalStep(world.committedLog(), workflowId,
                                                      BlockingAwaitTimeoutWorkflow.STEP_BLOCKING));
            Polling.awaitOrFail(Duration.ofSeconds(30),
                                "the blocking convenience call to surface an exception (captured by the body)",
                                () -> workflow.capturedSurface() != null);
            return workflow.capturedSurface();
        }
    }

    // ---- log readers (per (workflowId, stepName), content-based) ----

        private static Optional<Instant> startedAt(List<EventMessage> committedLog, String workflowId,
                                               String stepName) {
        return committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(s -> s == StepStatus.STARTED).orElse(false))
                .map(EventMessage::timestamp)
                .findFirst();
    }

    private static boolean hasStep(List<EventMessage> committedLog, String workflowId,
                                   String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).isPresent());
    }

    private static boolean hasTerminalStep(List<EventMessage> committedLog, String workflowId,
                                           String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(StepStatus::isTerminal).orElse(false));
    }
}
