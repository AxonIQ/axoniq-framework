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
package io.axoniq.framework.workflow.simulation.harness;

import io.axoniq.framework.workflow.simulation.faults.ClockJumpFault;
import io.axoniq.framework.workflow.simulation.faults.ClockSkewFault;
import io.axoniq.framework.workflow.simulation.faults.DuplicateCompletedFault;
import io.axoniq.framework.workflow.simulation.faults.DuplicatedAppendFault;
import io.axoniq.framework.workflow.simulation.faults.EventStoreLatencyJitterFault;
import io.axoniq.framework.workflow.simulation.faults.Fault;
import io.axoniq.framework.workflow.simulation.faults.FaultKind;
import io.axoniq.framework.workflow.simulation.faults.FlappingRestartFault;
import io.axoniq.framework.workflow.simulation.faults.MessageReorderFault;
import io.axoniq.framework.workflow.simulation.faults.PartialBatchFault;
import io.axoniq.framework.workflow.simulation.faults.RestartFault;
import io.axoniq.framework.workflow.simulation.faults.StaleWriterFault;
import io.axoniq.framework.workflow.simulation.faults.WorkerCrashFault;
import io.axoniq.framework.workflow.simulation.faults.WriteThenVanishFault;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.SimulationContext.PendingEvent;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CombinatorWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CorrelatedWaitWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CustomNamedWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.MigratingOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.LoopingPollWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.PayloadOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.ReducerWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SagaOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.VersionedOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.VersioningEdgesWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ApprovalGrantedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CombinatorRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedWaitRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CustomNamedRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.FulfillmentConfirmedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.LoopCounterPollRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.MigrateRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.OrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PayloadOrderRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PaymentConfirmedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PollSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ReducerRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RenewalDecidedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RollingDeployOrderEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.SagaRetryCompOrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.SubscriptionStartedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.VersionedOrderRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.VersioningEdgesRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.VersioningEdgesSignalEvent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * The seeded, single-threaded deterministic simulation loop. It drives the real engine through the Phase-3 seams under
 * injected faults and asserts the protocol invariants after every step (the leasing/crash-recovery set INV-1..6 plus
 * the DST-only INV-7 {@code TerminalIsFinal}, INV-8 {@code RetryBound}, INV-10 {@code OneInstancePerStart}, INV-11
 * {@code VersionRoutingSound}, INV-12 {@code MigrateVersionContract}, INV-13 {@code NoLostPayloadWrites}, INV-14
 * {@code CombinatorConsistency} and INV-15 {@code EventCorrelationExact}); a single {@link Random} seeded from the
 * config drives every fault choice, so a seed fully determines the run.
 * <p>
 * One step is: deliver any due external events → let the processor settle → advance virtual time a little → maybe
 * inject one seed-chosen fault → assert the always-on invariants. At the horizon every remaining event is delivered,
 * time is advanced past all timers, and liveness ({@code EventuallyTerminates}) plus the F-0 documentation are
 * asserted.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class DstSimulation {

    private static final Logger logger = LoggerFactory.getLogger(DstSimulation.class);
    // Per-settle budget: a SHORT bound on how long any single settle may wait for the async processor/body threads to
    // quiesce. It is always further clamped to whatever remains of the global wall-clock deadline (see Deadline), so no
    // single wait can outlive the run's hard stop. A healthy settle finishes in tens of milliseconds; this only caps a
    // pathological one before the global deadline takes over.
    private static final Duration SETTLE_BUDGET = Duration.ofSeconds(4);
    private static final Duration SETTLE_QUIET_WINDOW = Duration.ofMillis(120);
    // Small virtual-time nudge used inside settle to fire a pending sleep/retry-backoff timer. Many nudges across a
    // run stay well below the workflow's 365-day wait timeout, so awaitConfirmation never times out as a side effect.
    private static final Duration SETTLE_TIME_NUDGE = Duration.ofSeconds(2);
    // Cap nudges per settle so a never-due timer (e.g. a long wait timeout still pending) cannot make settle spin.
    private static final int MAX_NUDGES_PER_SETTLE = 30;
    private static final int HORIZON_ROUNDS = 8;
    // INV-8 (RetryBound): the configured maxRetries per retrying step of the fuzz workhorse (OrderWorkflow). The
    // assertion bounds attempt records (STARTED/RETRYING) at maxRetries+1; steps absent here carry no retry policy and
    // are not constrained. Taken from the workflow's own policy constant so the bound is not a magic number.
    private static final Map<String, Integer> RETRY_BOUNDS =
            Map.of(OrderWorkflow.STEP_SHIP_ORDER, OrderWorkflow.SHIP_ORDER_MAX_RETRIES);
    // INV-11 (VersionRoutingSound): the simulator starts ONE VersionedOrderWorkflow instance (registered at two
    // versions in the shared engine — see SimulationWorld#defaultRegistrations) alongside the order workhorse so version
    // routing is driven through the same crash/restart/reorder faults. A fresh start must spawn at the highest version
    // (VERSION_V2) and every committed event of the instance must carry exactly that one version. The id prefix scopes
    // the per-step assertion to this instance so the single-version order instances are not falsely flagged.
    private static final String VERSIONED_ORDER_ID = "v0";
    private static final String VERSIONED_ID_PREFIX = "vorder-";
    private static final String VERSIONED_WORKFLOW_ID = VERSIONED_ID_PREFIX + VERSIONED_ORDER_ID;
    // INV-12 (MigrateVersionContract): the simulator also starts ONE MigratingOrderWorkflow instance (registered as a
    // single definition in the shared engine — see SimulationWorld#defaultRegistrations) alongside the order workhorse.
    // Its body calls ctx.migrateVersion once, recording a migration marker that rides the same crash/restart/reorder
    // faults — exactly where a replay-stability bug in the migration record would show. The assertion (scoped to the
    // vmig- prefix) requires the recorded migration version be written at most once, monotonic non-decreasing, and
    // stable across replay.
    private static final String MIGRATING_ORDER_ID = "m0";
    private static final String MIGRATING_ID_PREFIX = "vmig-";
    private static final String MIGRATING_WORKFLOW_ID = MIGRATING_ID_PREFIX + MIGRATING_ORDER_ID;
    // INV-13 (NoLostPayloadWrites): the simulator also starts ONE PayloadOrderWorkflow instance (registered as a single
    // definition in the shared engine — see SimulationWorld#defaultRegistrations) alongside the order workhorse. Every
    // step of that workflow writes a DISTINCT payload key (three execute+combine merges then one modifyPayload replace),
    // so the final committed payload must reflect all four contributions even when a crash/restart/reorder fault
    // interrupts a write. The assertion (scoped to the payload- prefix) rebuilds the final payload from the committed
    // log exactly as the engine does and requires every committed contribution to survive (modulo a later same-key
    // overwrite) — exactly where a lost write across crash/replay would show.
    private static final String PAYLOAD_ORDER_ID = "p0";
    private static final String PAYLOAD_ID_PREFIX = "payload-";
    private static final String PAYLOAD_WORKFLOW_ID = PAYLOAD_ID_PREFIX + PAYLOAD_ORDER_ID;
    // INV-14 (CombinatorConsistency): the simulator also starts ONE CombinatorWorkflow instance (registered as a single
    // definition in the shared engine — see SimulationWorld#defaultRegistrations) alongside the order workhorse. It runs
    // three parallel execute branches and folds them through anyMatch/allMatch/noneMatch; each combinator's decision is
    // recorded both as a distinct post-combinator step name and as a payload key, riding the same crash/restart/reorder
    // faults — exactly where a replay that resolved a DIFFERENT combinator decision would show. The assertion (scoped to
    // the comb- prefix) re-derives the expected decision from the committed branch outcomes and requires the recorded
    // decision (step name + reconstructed payload) to match — consistent and stable across replay.
    private static final String COMBINATOR_ORDER_ID = "c0";
    private static final String COMBINATOR_ID_PREFIX = "comb-";
    private static final String COMBINATOR_WORKFLOW_ID = COMBINATOR_ID_PREFIX + COMBINATOR_ORDER_ID;
    // INV-15 (EventCorrelationExact): the simulator also starts TWO CorrelatedWaitWorkflow instances (registered as a
    // single definition in the shared engine — see SimulationWorld#defaultRegistrations) alongside the order workhorse,
    // each correlating its waitForEvent on a DISTINCT key (A and B). The harness delivers each instance's matching
    // CorrelatedSignalEvent through the pending batch — so the signals ride the SAME MESSAGE_REORDER reorder/delay/
    // DUPLICATE path as the order confirmations — exactly where a cross-wakeup (a signal for key A waking the instance
    // waiting on B) or a duplicate-driven second wait completion would show. The assertion (scoped to the corr- prefix)
    // requires every instance that completed its wait to have matched on its OWN key (matchedKey == ownKey) and to have
    // completed the wait at most once. Two instances on distinct keys is the minimum that makes cross-wakeup observable.
    private static final String CORRELATED_ID_PREFIX = "corr-";
    private static final String CORRELATED_ORDER_ID_A = "ka";
    private static final String CORRELATED_KEY_A = "keyA";
    private static final String CORRELATED_WORKFLOW_ID_A = CORRELATED_ID_PREFIX + CORRELATED_ORDER_ID_A;
    private static final String CORRELATED_ORDER_ID_B = "kb";
    private static final String CORRELATED_KEY_B = "keyB";
    private static final String CORRELATED_WORKFLOW_ID_B = CORRELATED_ID_PREFIX + CORRELATED_ORDER_ID_B;
    // INV-19 (PayloadReducerSemantics): the simulator also starts ONE ReducerWorkflow instance (registered as a single
    // definition in the shared engine — see SimulationWorld#defaultRegistrations) alongside the order workhorse. Its
    // steps deterministically exercise all three payload reducers (a combine seed, a local_only modifyPayload replace, a
    // combine that must appear, a global_only execute whose result must be discarded, and a
    // parameterPayloadReducer(combine) step demonstrating the parameter-side input view), riding the same crash/restart/
    // reorder faults — exactly where a reducer mis-application or a replay that rebuilds a different payload would show.
    // The assertion (scoped to the reducer- prefix) requires the engine's reconstructed payload to equal the documented
    // reducer fold of the committed log — content-based, F-2-robust, stable across replay.
    private static final String REDUCER_ORDER_ID = "r0";
    private static final String REDUCER_ID_PREFIX = "reducer-";
    private static final String REDUCER_WORKFLOW_ID = REDUCER_ID_PREFIX + REDUCER_ORDER_ID;
    // INV-20 (VersioningEdges): the simulator also starts ONE VersioningEdgesWorkflow instance (registered at THREE
    // versions in the shared engine — see SimulationWorld#defaultRegistrations) alongside the order workhorse. Its body
    // performs two forward ctx.migrateVersion bumps under distinct changeIds and a rejected downgrade, then waits for a
    // VersioningEdgesSignalEvent (delivered by the harness so it self-completes). The per-step assertion (scoped to the
    // vedge- prefix) requires: no migration marker for the downgrade changeId (the downgrade was rejected, never
    // recorded), each (workflowId, changeId) marker recorded at most once and monotonic non-decreasing, and the instance
    // resolved to exactly ONE version at the HIGHEST of the three registered versions (a fresh spawn), stable across the
    // crash/restart/reorder faults. (The deeper closest-sibling routing facet — recovery under a registry missing the
    // recorded version — needs crashAndRecoverWith and is exercised by the deterministic Inv20VersioningEdgesTest, not
    // the fixed-registration fuzz loop.)
    private static final String VERSIONING_EDGES_ORDER_ID = "e0";
    private static final String VERSIONING_EDGES_ID_PREFIX = "vedge-";
    private static final String VERSIONING_EDGES_WORKFLOW_ID = VERSIONING_EDGES_ID_PREFIX + VERSIONING_EDGES_ORDER_ID;
    // INV-22 (EventNameCustomizationSound): the simulator also starts ONE CustomNamedWorkflow instance (registered with
    // a custom eventNameCustomizer in the shared engine — see SimulationWorld#defaultRegistrations) alongside the order
    // workhorse. It self-completes via two execute steps; every committed step/status event it emits must carry the
    // EXPECTED customized wire name (the custom namespace + the customizer-derived local name), and those names must stay
    // identical across the crash/restart/reorder faults (a pure function of history). The per-step assertion (scoped to
    // the named- prefix) requires each event's QualifiedName to equal the customizer-derived expected — exactly where a
    // customized name NOT applied, or a replay producing a DIFFERENT name under customization, would show.
    private static final String CUSTOM_NAMED_ORDER_ID = "n0";
    private static final String CUSTOM_NAMED_ID_PREFIX = "named-";
    private static final String CUSTOM_NAMED_WORKFLOW_ID = CUSTOM_NAMED_ID_PREFIX + CUSTOM_NAMED_ORDER_ID;

    // P-series production-realism singletons (see SimulationWorld#defaultRegistrations). Each waits on its own
    // external event, delivered through the pending batch (riding the MESSAGE_REORDER reorder/delay/duplicate path)
    // and re-delivered at-least-once while non-terminal — the same INV-5 assumption the order workhorse rides.
    private static final String SAGA_ORDER_ID = "p1";
    private static final String SAGA_WORKFLOW_ID = "sagarc-" + SAGA_ORDER_ID;
    private static final String SUBSCRIPTION_ORDER_ID = "p2";
    private static final String SUBSCRIPTION_WORKFLOW_ID = "subs-" + SUBSCRIPTION_ORDER_ID;
    private static final String DEPLOY_ORDER_ID = "p3";
    private static final String DEPLOY_WORKFLOW_ID = "deploy-" + DEPLOY_ORDER_ID;
    private static final String LOOP_ORDER_ID = "p4";
    private static final String LOOP_WORKFLOW_ID = "loopc-" + LOOP_ORDER_ID;

    private final SimulationConfig config;
    private final Map<FaultKind, Fault> faults = new EnumMap<>(FaultKind.class);

    /**
     * Creates a simulation for the given configuration.
     *
     * @param config the run configuration (seed, instance count, steps, fault probability).
     */
    public DstSimulation(SimulationConfig config) {
        this.config = config;
        faults.put(FaultKind.WORKER_CRASH, new WorkerCrashFault());
        faults.put(FaultKind.MESSAGE_REORDER, new MessageReorderFault());
        faults.put(FaultKind.RESTART, new RestartFault());
        faults.put(FaultKind.CLOCK_JUMP, new ClockJumpFault());
        faults.put(FaultKind.WRITE_THEN_VANISH, new WriteThenVanishFault());
        // Chaos faults (only drawn when the config's faultKinds() includes them, e.g. SimulationConfig.chaos).
        faults.put(FaultKind.DUPLICATE_COMPLETED, new DuplicateCompletedFault());
        faults.put(FaultKind.EVENT_STORE_LATENCY_JITTER, new EventStoreLatencyJitterFault());
        faults.put(FaultKind.FLAPPING_RESTART, new FlappingRestartFault());
        faults.put(FaultKind.CLOCK_SKEW, new ClockSkewFault());
        faults.put(FaultKind.PARTIAL_BATCH, new PartialBatchFault());
        faults.put(FaultKind.DUPLICATED_APPEND, new DuplicatedAppendFault());
        faults.put(FaultKind.STALE_WRITER, new StaleWriterFault());
    }

    /**
     * Runs the simulation to completion and returns what it observed. Throws
     * {@link InvariantViolation} (enriched with the seed + committed log) on
     * any genuine invariant break.
     *
     * @return the run result (committed log, fingerprint, effect counts, fault trace, F-0 flag).
     */
        public SimulationResult run() {
        // PRIMARY anti-hang stop: a hard real-time deadline for the whole run. The loop checks it every iteration and
        // every bounded wait clamps to what remains, so the harness cannot hang regardless of engine behaviour.
        var deadline = new Deadline(config.wallClockDeadline());
        // Phase 2: optional deterministic single-carrier body executor (FIFO or seeded interleaving pick).
        var carrier = config.deterministicCarrier()
                ? (config.interleavingSeed() == null
                           ? new io.axoniq.framework.workflow.runtime.test.fakes.DeterministicVirtualThreadExecutor()
                           : new io.axoniq.framework.workflow.runtime.test.fakes.DeterministicVirtualThreadExecutor(
                                   config.interleavingSeed()))
                : null;
        try (var closeCarrier = carrier;
             var world = carrier == null
                     ? new SimulationWorld(config.seed())
                     : new SimulationWorld(config.seed(),
                                           (java.util.List<EngineInstance.WorkflowRegistration>) null,
                                           carrier,
                                           false)) {
            var rng = new Random(config.seed());
            var pending = new ArrayList<PendingEvent>();
            var context = new SimulationContext(world, rng, pending);
            // Phase 3: liveness is also accounted in VIRTUAL time — how much simulated time the run consumed.
            var virtualStart = world.clock().instant();

            var orderIds = new ArrayList<String>();
            var releaseStep = new java.util.HashMap<String, Integer>();
            for (int i = 0; i < config.workflowCount(); i++) {
                String orderId = "wf" + i;
                orderIds.add(orderId);
                // The external confirmation for this order is released at a seed-chosen early step (0 or 1) and then
                // re-delivered every step (at-least-once). A small release delay still lets the workflow genuinely
                // reach awaitConfirmation, but keeping it small minimizes the window in which a crash could strand a
                // freshly-resumed waiter before the next redelivery.
                releaseStep.put(orderId, rng.nextInt(2));
            }

            // Start all workflows (deterministic order).
            for (String orderId : orderIds) {
                world.engine().publish(new OrderPlacedEvent(orderId));
            }
            // INV-11: also start the single VersionedOrderWorkflow instance. It self-completes via execute steps (no
            // external wait), spawning at the highest registered version; the per-step assertion checks every event of
            // this instance carries exactly that one version.
            world.engine().publish(new VersionedOrderRequestedEvent(VERSIONED_ORDER_ID));
            // INV-12: also start the single MigratingOrderWorkflow instance. It self-completes via execute steps; its
            // body calls ctx.migrateVersion once, recording a migration marker the per-step assertion checks is written
            // at most once, monotonic, and stable across the crash/restart/reorder faults.
            world.engine().publish(new MigrateRequestedEvent(MIGRATING_ORDER_ID));
            // INV-13: also start the single PayloadOrderWorkflow instance. It self-completes via execute/modifyPayload
            // steps, each writing a distinct payload key; the per-step assertion rebuilds the final committed payload
            // and checks every committed contribution survives across the crash/restart/reorder faults (no lost write).
            world.engine().publish(new PayloadOrderRequestedEvent(PAYLOAD_ORDER_ID));
            // INV-14: also start the single CombinatorWorkflow instance. It self-completes via execute steps (three
            // parallel branches then anyMatch/allMatch/noneMatch); the per-step assertion re-derives each combinator's
            // expected decision from its committed branch outcomes and checks the recorded decision (step name +
            // reconstructed payload) matches and stays consistent across the crash/restart/reorder faults.
            world.engine().publish(new CombinatorRequestedEvent(COMBINATOR_ORDER_ID));
            // INV-15: also start TWO CorrelatedWaitWorkflow instances, each waiting on a DISTINCT correlation key (A, B).
            // The harness delivers each instance's matching CorrelatedSignalEvent through the pending batch (ride the
            // MESSAGE_REORDER reorder/delay/duplicate path); the per-step assertion checks every completed wait matched
            // on its OWN key (no cross-wakeup) and completed at most once.
            world.engine().publish(new CorrelatedWaitRequestedEvent(CORRELATED_ORDER_ID_A, CORRELATED_KEY_A));
            world.engine().publish(new CorrelatedWaitRequestedEvent(CORRELATED_ORDER_ID_B, CORRELATED_KEY_B));
            // INV-19: also start the single ReducerWorkflow instance. It self-completes via execute/modifyPayload steps
            // that exercise all three payload reducers; the per-step assertion checks the engine's reconstructed payload
            // equals the documented reducer fold of the committed log across the crash/restart/reorder faults.
            world.engine().publish(new ReducerRequestedEvent(REDUCER_ORDER_ID));
            // INV-20: also start the single VersioningEdgesWorkflow instance (registered at three versions). Its body
            // performs two forward ctx.migrateVersion bumps under distinct changeIds and a rejected downgrade, then waits
            // for a VersioningEdgesSignalEvent (delivered below so it self-completes); the per-step assertion checks the
            // downgrade is never recorded, each changeId marker is written at most once and monotonic, and the instance
            // resolves to exactly one version at the highest of the three, across the crash/restart/reorder faults.
            world.engine().publish(new VersioningEdgesRequestedEvent(VERSIONING_EDGES_ORDER_ID));
            // INV-22: also start the single CustomNamedWorkflow instance. It self-completes via execute steps; every
            // committed step/status event it emits must carry the EXPECTED customized wire name (custom namespace +
            // customizer-derived local name), stable across the crash/restart/reorder faults.
            world.engine().publish(new CustomNamedRequestedEvent(CUSTOM_NAMED_ORDER_ID));
            // P-series: the compensation saga (retry-comp variant, happy mode — its fulfillment confirmation is
            // delivered through the batch), the day-scale parked subscription (its renewal decision likewise), the
            // rolling-deploy v1+v2 sibling registry (fresh spawn at v2 through the migrateVersion gate; approval
            // likewise), and the counter-names retry loop (its poll signal likewise).
            world.engine().publish(new SagaRetryCompOrderPlacedEvent(SAGA_ORDER_ID, SagaOrderWorkflow.MODE_HAPPY));
            world.engine().publish(new SubscriptionStartedEvent(SUBSCRIPTION_ORDER_ID));
            world.engine().publish(new RollingDeployOrderEvent(DEPLOY_ORDER_ID));
            world.engine().publish(new LoopCounterPollRequestedEvent(LOOP_ORDER_ID));
            settle(world, "all workflows started", context, deadline);

            Map<String, List<String>> previousPerWorkflow = Map.of();
            // SETTLE-AWARE step cap (the seed-339-class load-residual fix). The max-steps cap is the SECONDARY
            // anti-hang guard (the wall-clock deadline is primary), but it used to burn one unit of cap per loop
            // iteration even when the only "problem" was that the engine's durably-async work lagged the loop under
            // machine load (settle returning non-quiescent) — so a loaded nightly run could spuriously abort at the
            // cap on a seed that passes in isolation. The cap is now charged in EFFECTIVE steps: only an iteration
            // whose settle reached genuine quiescence (committed log stable for the quiet window, pending timers
            // either drained or established never-due — settle's own outcome) counts against config.maxSteps(); a
            // non-quiescent iteration (work still visibly in flight) does not burn the cap.
            //
            // REPRODUCIBILITY IS PRESERVED BY CONSTRUCTION: the iteration structure and the per-iteration fault draws
            // are EXACTLY as before — every iteration still builds the batch, draws from the seeded RNG once
            // (maybeInjectFault) and delivers, in the same order; quiescence only changes the ABORT decision (which
            // iterations are charged against the cap), never the RNG-driving sequence. A healthy run terminates on
            // all-instances-terminal exactly as before (same iterations, same draws); only a run that would
            // previously have spuriously aborted at the cap under load now keeps going. The quiescence signal is
            // wall-clock-influenced (that is the point — it ABSORBS load), which is safe precisely because it feeds
            // only the cap accounting, not the seeded draw sequence.
            //
            // The loop stays provably finite even if quiescence detection were wrong: the wall-clock deadline remains
            // the hard stop (checked every iteration), and a generous absolute iteration bound (10x maxSteps) is the
            // final backstop.
            final int iterationBound = config.maxSteps() * 10;
            int step = 0;            // raw iteration index — drives the RNG/fault/trace exactly as before
            int effectiveSteps = 0;  // quiescent iterations — the currency the max-steps cap is charged in
            for (; step < iterationBound && effectiveSteps < config.maxSteps()
                    && !allNonTerminal(world, orderIds).isEmpty(); step++) {
                deadline.checkNotExpired(world, context, "main loop step " + step);
                // Build this step's confirmation batch (released, still-non-terminal orders) into the pending list so
                // the MESSAGE_REORDER fault can reorder/delay/duplicate it before delivery.
                buildConfirmationBatch(world, orderIds, releaseStep, step, pending);
                maybeInjectFault(context, step);
                deliverBatch(world, pending, context);
                if (settle(world, "step " + step, context, deadline)) {
                    effectiveSteps++;
                }

                previousPerWorkflow = assertAlwaysOnInvariants(world, previousPerWorkflow, context);
            }
            // SECOND anti-hang guard: exhausting the (effective) step cap — or the absolute iteration backstop — with
            // instances still non-terminal is a potential liveness finding (a spin or a stall), not a silent success —
            // abort with the full diagnostic.
            if (!allNonTerminal(world, orderIds).isEmpty()
                    && (effectiveSteps >= config.maxSteps() || step >= iterationBound)) {
                throw abort("EventuallyTerminates",
                            "max-steps cap reached (" + effectiveSteps + " effective/quiescent steps of cap "
                                    + config.maxSteps() + " over " + step + " loop iterations, iteration backstop "
                                    + iterationBound + ") with non-terminal instance(s) "
                                    + allNonTerminal(world, orderIds) + " — potential liveness finding (spin/stall)",
                            world, context);
            }

            // Horizon: drive any remaining instance to quiescence — re-deliver confirmations (idempotent for
            // instances already past awaitConfirmation) and let settle fire every pending timer, repeating until all
            // instances are terminal or a bounded number of rounds elapse. This is the fair-scheduling +
            // at-least-once-delivery assumption INV-5 makes.
            for (int round = 0; round < HORIZON_ROUNDS && !allNonTerminal(world, orderIds).isEmpty(); round++) {
                deadline.checkNotExpired(world, context, "horizon round " + round);
                // Re-deliver each non-terminal order's confirmation a few times so a workflow that is mid-resume
                // (replaying after a crash) is still waiting when at least one delivery lands, then settle to let it
                // run to completion. At-least-once delivery + fair scheduling is exactly the INV-5 assumption.
                for (int repeat = 0; repeat < 3; repeat++) {
                    for (String orderId : nonTerminalOrderIds(world, orderIds)) {
                        world.engine().publish(new PaymentConfirmedEvent(orderId));
                    }
                    // INV-15: re-deliver each non-terminal correlated-wait instance's OWN-key signal so a wait that is
                    // mid-resume (replaying after a crash) is still waiting when at least one delivery lands. Each
                    // instance only ever sees its own key, so this completes the right wait without any cross-wakeup.
                    if (!isTerminalInLog(world.committedLog(), CORRELATED_WORKFLOW_ID_A)) {
                        world.engine().publish(new CorrelatedSignalEvent(CORRELATED_KEY_A));
                    }
                    if (!isTerminalInLog(world.committedLog(), CORRELATED_WORKFLOW_ID_B)) {
                        world.engine().publish(new CorrelatedSignalEvent(CORRELATED_KEY_B));
                    }
                    // INV-20: re-deliver the versioning-edges instance's awaited signal so a wait that is mid-resume
                    // (replaying after a crash) is still waiting when at least one delivery lands.
                    if (!isTerminalInLog(world.committedLog(), VERSIONING_EDGES_WORKFLOW_ID)) {
                        world.engine().publish(new VersioningEdgesSignalEvent(VERSIONING_EDGES_ORDER_ID));
                    }
                    // P-series singletons: same at-least-once horizon re-delivery for each non-terminal instance.
                    if (!isTerminalInLog(world.committedLog(), SAGA_WORKFLOW_ID)) {
                        world.engine().publish(new FulfillmentConfirmedEvent(SAGA_ORDER_ID));
                    }
                    if (!isTerminalInLog(world.committedLog(), SUBSCRIPTION_WORKFLOW_ID)) {
                        world.engine().publish(new RenewalDecidedEvent(SUBSCRIPTION_ORDER_ID));
                    }
                    if (!isTerminalInLog(world.committedLog(), DEPLOY_WORKFLOW_ID)) {
                        world.engine().publish(new ApprovalGrantedEvent(DEPLOY_ORDER_ID));
                    }
                    if (!isTerminalInLog(world.committedLog(), LOOP_WORKFLOW_ID)) {
                        world.engine().publish(new PollSignalEvent(LOOP_ORDER_ID));
                    }
                    settle(world, "horizon round " + round + " repeat " + repeat, context, deadline);
                }
                context.record("HORIZON round " + round + ": re-delivered confirmations to non-terminal orders");
            }

            assertAlwaysOnInvariants(world, previousPerWorkflow, context);
            assertLivenessAndDocumentGaps(world, orderIds, context, deadline);

            var effectCounts = world.effects().snapshot();
            // F-0 (effect duplication) is an effect running >1×. The shipOrder step legitimately runs its effect
            // multiple times by design (it retries — INV-8 RetryBound), so it is NOT an F-0 signal; only a non-retry
            // step running more than once indicates the F-0 crash-window duplication.
            // Steps carrying a retry policy legitimately re-run their effect per attempt (per-attempt at-most-once):
            // shipOrder, and the saga's retried charge + retry-compensation steps. The loop workload's iteration
            // counter is a BODY-level diagnostic (it deliberately counts body re-entries, which a crash/replay re-run
            // legitimately repeats), not a step effect — excluded too.
            boolean observedF0 = effectCounts.entrySet().stream()
                    .anyMatch(e -> e.getValue() > 1
                            && !e.getKey().endsWith("/" + OrderWorkflow.STEP_SHIP_ORDER)
                            && !e.getKey().endsWith("/" + SagaOrderWorkflow.STEP_CHARGE_PAYMENT)
                            && !e.getKey().endsWith("/" + SagaOrderWorkflow.STEP_RELEASE_STOCK)
                            && !e.getKey().endsWith("/" + SagaOrderWorkflow.STEP_REFUND_PAYMENT)
                            && !e.getKey().endsWith("/" + LoopingPollWorkflow.EFFECT_ITERATION));
            return new SimulationResult(
                    config.seed(),
                    world.committedLog(),
                    fingerprint(world.committedLog()),
                    effectCounts,
                    context.trace(),
                    observedF0,
                    java.time.Duration.between(virtualStart, world.clock().instant()));
        }
    }

    /**
     * Rebuilds the {@code pending} batch for this step: one {@link PaymentConfirmedEvent} per order that has reached
     * its release step and is not yet terminal. Any events a previous step's MESSAGE_REORDER fault delayed
     * ({@code delaySteps > 0}) are carried over (aged) so a delay actually defers delivery.
     */
    private void buildConfirmationBatch(SimulationWorld world, List<String> orderIds,
                                        Map<String, Integer> releaseStep, int step,
                                        List<PendingEvent> pending) {
        var carried = pending.stream().filter(p -> p.delaySteps() > 0).map(PendingEvent::aged).toList();
        pending.clear();
        pending.addAll(carried);
        var nonTerminal = nonTerminalOrders(world, orderIds);
        for (String orderId : orderIds) {
            String workflowId = "order-" + orderId;
            if (step >= releaseStep.getOrDefault(orderId, 0) && nonTerminal.contains(workflowId)) {
                pending.add(new PendingEvent(new PaymentConfirmedEvent(orderId), 0,
                                             "PaymentConfirmed(" + orderId + ")"));
            }
        }
        // INV-15: enqueue each non-terminal correlated-wait instance's MATCHING signal (key A for instance A, key B for
        // instance B) into the same pending batch, so it rides the MESSAGE_REORDER reorder/delay/DUPLICATE path. A
        // signal for key A must wake ONLY the instance waiting on A — never the one waiting on B (no cross-wakeup) — and
        // a duplicate of an already-matched signal must not complete the wait twice. Each is correlated by its own key,
        // so a signal for one key reaching the other instance's body would be an engine cross-wakeup bug.
        if (!isTerminalInLog(world.committedLog(), CORRELATED_WORKFLOW_ID_A)) {
            pending.add(new PendingEvent(new CorrelatedSignalEvent(CORRELATED_KEY_A), 0,
                                         "CorrelatedSignal(" + CORRELATED_KEY_A + ")"));
        }
        if (!isTerminalInLog(world.committedLog(), CORRELATED_WORKFLOW_ID_B)) {
            pending.add(new PendingEvent(new CorrelatedSignalEvent(CORRELATED_KEY_B), 0,
                                         "CorrelatedSignal(" + CORRELATED_KEY_B + ")"));
        }
        // INV-20: enqueue the versioning-edges instance's awaited signal (correlated on its orderId) into the same
        // pending batch so it rides the MESSAGE_REORDER reorder/delay/duplicate path; it wakes the vedge instance's
        // awaitSignal wait so the instance can run its final step and self-complete.
        if (!isTerminalInLog(world.committedLog(), VERSIONING_EDGES_WORKFLOW_ID)) {
            pending.add(new PendingEvent(new VersioningEdgesSignalEvent(VERSIONING_EDGES_ORDER_ID), 0,
                                         "VersioningEdgesSignal(" + VERSIONING_EDGES_ORDER_ID + ")"));
        }
        // P-series singletons: each non-terminal instance's own wake event rides the same reorder/delay/duplicate
        // batch, re-enqueued every step (at-least-once) so a wake lost to the F-16 recovery gap is re-delivered.
        if (!isTerminalInLog(world.committedLog(), SAGA_WORKFLOW_ID)) {
            pending.add(new PendingEvent(new FulfillmentConfirmedEvent(SAGA_ORDER_ID),
                                         0, "FulfillmentConfirmed(" + SAGA_ORDER_ID + ")"));
        }
        if (!isTerminalInLog(world.committedLog(), SUBSCRIPTION_WORKFLOW_ID)) {
            pending.add(new PendingEvent(new RenewalDecidedEvent(SUBSCRIPTION_ORDER_ID),
                                         0, "RenewalDecided(" + SUBSCRIPTION_ORDER_ID + ")"));
        }
        if (!isTerminalInLog(world.committedLog(), DEPLOY_WORKFLOW_ID)) {
            pending.add(new PendingEvent(new ApprovalGrantedEvent(DEPLOY_ORDER_ID),
                                         0, "ApprovalGranted(" + DEPLOY_ORDER_ID + ")"));
        }
        if (!isTerminalInLog(world.committedLog(), LOOP_WORKFLOW_ID)) {
            pending.add(new PendingEvent(new PollSignalEvent(LOOP_ORDER_ID),
                                         0, "PollSignal(" + LOOP_ORDER_ID + ")"));
        }
    }

    /**
     * Publishes every batch entry whose delay has elapsed, removing it; delayed entries stay in {@code pending} for a
     * later step.
     */
    private void deliverBatch(SimulationWorld world, List<PendingEvent> pending,
                              SimulationContext context) {
        var due = pending.stream().filter(p -> p.delaySteps() == 0).toList();
        pending.removeAll(due);
        for (PendingEvent event : due) {
            world.engine().publish(event.event());
            context.record("DELIVER: " + event.description());
        }
    }

    private void maybeInjectFault(SimulationContext context, int step) {
        if (context.rng().nextDouble() >= config.faultProbability()) {
            context.record("step " + step + ": NONE");
            return;
        }
        FaultKind kind = config.faultKinds()[context.rng().nextInt(config.faultKinds().length)];
        logger.debug("seed={} step={} injecting fault {}", config.seed(), step, kind);
        try {
            faults.get(kind).apply(context);
        } catch (InvariantViolation violation) {
            // A fault that asserts an invariant in-line (e.g. WorkerCrashFault / RestartFault checking
            // CommittedHistorySurvivesCrash across recovery) must surface the same enriched diagnostic as every other
            // break: seed + reproduce command + fault trace + committed log. Without this, a fault-raised violation
            // would propagate bare and the engineer would have no seed/reproduce line to act on.
            throw enrich(violation, context.world(), context);
        }
    }

        private Map<String, List<String>> assertAlwaysOnInvariants(SimulationWorld world,
                                                               Map<String, List<String>> previousPerWorkflow,
                                                               SimulationContext context) {
        var committedLog = world.committedLog();
        var currentPerWorkflow = perWorkflowEvents(committedLog);
        try {
            // INV-1: a single engine process — trivially at most one owner.
            Invariants.assertAtMostOneOwner(1);
            // INV-2: no step recorded twice as terminal.
            Invariants.assertAtMostOnceRecording(committedLog);
            // INV-7: termination is final — once an instance records a terminal workflow status, no further step or
            // status event is recorded for it (per-workflowId; holds across the crash/recover faults too).
            Invariants.assertTerminalIsFinal(committedLog);
            // INV-10: one instance per start — a duplicate/redelivered start (the MESSAGE_REORDER duplicate mode) must
            // not open a second concurrent LIVE instance for a workflowId (per-workflowId: no second workflow-status
            // STARTED without an intervening terminal status). The after-terminal re-spawn (F-3) is tolerated — only a
            // genuine live-dedup failure (two concurrent live instances) throws.
            Invariants.assertOneInstancePerStart(committedLog);
            // INV-8: retry bound — the OrderWorkflow shipOrder step genuinely retries (maxRetries=2), so its attempt
            // records (STARTED/RETRYING) must stay ≤ maxRetries+1 per (workflowId, stepName), even when a crash/restart
            // fault interrupts a retry. Record-level (cf. INV-6/F-0 which is effect-level).
            Invariants.assertRetryBound(committedLog, RETRY_BOUNDS);
            // INV-11: version routing is sound — the VersionedOrderWorkflow instance (registered at two versions) must
            // route to exactly ONE definition at the HIGHEST registered version (VERSION_V2), and every committed event
            // it emits must carry exactly that one version, deterministically across the crash/restart/reorder faults.
            // Scoped to the vorder- prefix so the single-version order instances are not flagged. No presence is
            // required mid-run (Set.of()) — the start may still be in flight; the horizon check below requires presence.
            Invariants.assertVersionRoutingSound(committedLog, VERSIONED_ID_PREFIX, VersionedOrderWorkflow.VERSION_V2,
                                                 Set.of());
            // INV-12: migrateVersion contract — the MigratingOrderWorkflow instance's in-body ctx.migrateVersion must
            // record its changeId's version at most once, monotonic non-decreasing (never downgraded), and stable
            // across the crash/restart/reorder faults (replay re-reaches the call as a no-op). Scoped to the vmig-
            // prefix so only the migrating instance is checked. Stable across replay is enforced jointly with the
            // per-instance prefix-stability check below (INV-4) — the migration marker lives on the instance's
            // committed subsequence, which only ever grows.
            Invariants.assertMigrateVersionContract(committedLog, MIGRATING_ID_PREFIX);
            // INV-13: no lost payload writes — the PayloadOrderWorkflow instance writes a distinct payload key per step
            // (three execute+combine merges then one modifyPayload replace). Every committed contribution (from the log)
            // must be reflected in the engine's OWN reconstructed payload (the history read-model's state), even when a
            // crash/restart/reorder fault interrupts a write — modulo a later same-key overwrite. The two
            // reconstructions are independent (raw log fold vs the projector's evolution), so a dropped/lost write would
            // make them diverge. Scoped to the payload- prefix so only the payload instance is checked. Stable across
            // replay is enforced jointly with the per-instance prefix-stability check below (INV-4) — the payload is a
            // pure function of the instance's committed subsequence, which only ever grows, and the projector re-derives
            // it across the crash/replay.
            Invariants.assertNoLostPayloadWrites(committedLog, PAYLOAD_ID_PREFIX,
                                                 world.engine().reconstructedPayloads(PAYLOAD_ID_PREFIX));
            // INV-14: combinator consistency — the CombinatorWorkflow instance runs three parallel branches then folds
            // them through anyMatch/allMatch/noneMatch. Each combinator's recorded decision (the distinct post-combinator
            // step name + the reconstructed-payload boolean) must equal what the documented short-circuit semantics
            // dictate over that instance's committed branch outcomes — even when a crash/restart/reorder fault interrupts
            // a branch or the recording. The expected decision is re-derived from the committed branch votes (an
            // independent source from the recorded decision), so a combinator resolving the wrong decision or rebuilding
            // a different one on replay would diverge. Scoped to the comb- prefix so only the combinator instance is
            // checked. Stable across replay is enforced jointly with the per-instance prefix-stability check below
            // (INV-4) — the decision lives on the instance's committed subsequence, which only ever grows, and the
            // projector re-derives the payload across the crash/replay.
            Invariants.assertCombinatorConsistency(committedLog, COMBINATOR_ID_PREFIX,
                                                   world.engine().reconstructedPayloads(COMBINATOR_ID_PREFIX));
            // INV-15: event correlation is exact — the two CorrelatedWaitWorkflow instances (waiting on distinct keys A
            // and B) must each complete their wait ONLY on their own key's signal: every instance that completed its
            // wait recorded matchedKey == its own correlation key (a foreign key = cross-wakeup = the §10 anti-pattern),
            // and the wait completed at most once for the right key even when the MESSAGE_REORDER duplicate mode
            // redelivers the matching signal. Scoped to the corr- prefix so only the correlated-wait instances are
            // checked. Cross-checked against the engine's OWN reconstructed payload as a second independent source.
            Invariants.assertEventCorrelationExact(committedLog, CORRELATED_ID_PREFIX,
                                                   world.engine().reconstructedPayloads(CORRELATED_ID_PREFIX));
            // INV-19: payload reducer semantics — the ReducerWorkflow instance's steps exercise all three payload
            // reducers (a combine seed, a local_only modifyPayload replace dropping the seed, a combine that must appear,
            // a global_only execute whose result must be discarded, and a parameterPayloadReducer(combine) step). The
            // engine's OWN reconstructed payload must equal the documented reducer fold of the committed log (combine
            // merges, local_only replaces, global_only discards) — even when a crash/restart/reorder fault interrupts a
            // write. The two reconstructions are independent (raw log fold vs the projector's evolution), so a reducer
            // mis-application or a replay that rebuilds a different payload would make them diverge. Scoped to the
            // reducer- prefix so only the reducer instance is checked. Content-based (the engine's reconstructed
            // payload), never asserting the instance's own global-append order — F-2-robust, exactly like INV-13. The
            // expected fold (A) is reconstructed in the instance's DETERMINISTIC LOGICAL step order (not the global-append
            // order) so a RESTART that re-appends a local_only modifyPayload COMPLETED ahead of an earlier combine step's
            // delayed COMPLETED cannot make (A) resurrect a dropped key — the seed-150/317-class load-flake. Stable
            // across replay is enforced jointly with the per-instance prefix-stability check below (INV-4).
            Invariants.assertPayloadReducerSemantics(committedLog, REDUCER_ID_PREFIX,
                                                     world.engine().reconstructedPayloads(REDUCER_ID_PREFIX));
            // INV-20: versioning edges — the VersioningEdgesWorkflow instance (registered at three versions) must never
            // record a marker for the downgrade changeId (the downgrade was rejected, never recorded), record each
            // (changeId) marker at most once and monotonic non-decreasing, and resolve to exactly ONE version at the
            // HIGHEST of the three registered versions (a fresh spawn) — across the crash/restart/reorder faults. Scoped
            // to the vedge- prefix. requireHighestForFresh=true (the fuzz instance is always a fresh spawn); no presence
            // required mid-run (Set.of()). The deeper closest-sibling recovery facet is in Inv20VersioningEdgesTest.
            Invariants.assertVersioningEdges(committedLog, VERSIONING_EDGES_ID_PREFIX,
                                             VersioningEdgesWorkflow.VERSION_HIGH,
                                             VersioningEdgesWorkflow.CHANGE_ID_DOWNGRADE, true, Set.of());
            // INV-22: event-name customization is sound — the CustomNamedWorkflow instance (registered with a custom
            // eventNameCustomizer) must record every committed step/status event under the EXPECTED customized wire name
            // (custom namespace + the customizer-derived local name: capitalize(stepName)+suffix for steps, the workflow
            // base name + suffix for statuses, with the stepCompleted->"Done" override), and those names must stay
            // identical across the crash/restart/reorder faults (a pure function of history). Scoped to the named-
            // prefix so only the custom-named instance is checked; no presence required mid-run (Set.of()) — the start
            // may still be in flight; the horizon check below requires presence + terminal. Stable across replay is
            // enforced jointly with the per-instance prefix-stability check below (INV-4) — each customized name lives
            // on the instance's committed subsequence, which only ever grows, and a replayed present step emits nothing.
            Invariants.assertEventNameCustomizationSound(committedLog, CUSTOM_NAMED_ID_PREFIX, Set.of());
            // INV-4 (intra-run): each instance's committed subsequence only ever grows — a previously committed
            // prefix is never rewritten or lost across a step (the cross-instance interleaving of the single global
            // log is the F-2 surface and is not asserted here; per-instance order is what replay determinism means).
            assertPerWorkflowPrefixStable(previousPerWorkflow, currentPerWorkflow);
        } catch (InvariantViolation violation) {
            throw enrich(violation, world, context);
        }
        return currentPerWorkflow;
    }

    private void assertPerWorkflowPrefixStable(Map<String, List<String>> previous,
                                               Map<String, List<String>> current) {
        for (var entry : previous.entrySet()) {
            var workflowId = entry.getKey();
            var before = entry.getValue();
            var now = current.getOrDefault(workflowId, List.of());
            if (now.size() < before.size()) {
                throw new InvariantViolation(
                        "DeterministicReplay",
                        "committed subsequence for '" + workflowId + "' shrank (" + before.size() + " -> " + now.size()
                                + ") — a committed record was lost or rewritten across a step");
            }
            for (int i = 0; i < before.size(); i++) {
                if (!before.get(i).equals(now.get(i))) {
                    throw new InvariantViolation(
                            "DeterministicReplay",
                            "committed subsequence for '" + workflowId + "' changed at index " + i + ": was '"
                                    + before.get(i) + "', now '" + now.get(i) + "'");
                }
            }
        }
    }

    private void assertLivenessAndDocumentGaps(SimulationWorld world, List<String> orderIds,
                                               SimulationContext context, Deadline deadline) {
        try {
            // INV-5: every instance reached a terminal status by the horizon. The authoritative durable signal is a
            // terminal workflow-status event in the committed log; the in-memory live set drains asynchronously after
            // completion, so we poll it down rather than reading it once (a lingering entry is a race, not a stall).
            // The poll is bounded by what remains of the global deadline so it can never block past the hard stop.
            Polling.await(deadline.clamp(SETTLE_BUDGET), () -> world.engine().liveWorkflowIds().isEmpty());
            // The versioned instance is included so its non-termination is a liveness failure too (it self-completes via
            // execute steps, so a stuck one would signal a routing/replay regression, not an intended wait).
            Invariants.assertEventuallyTerminates(allNonTerminal(world, orderIds));
            // INV-11 at the horizon: now the versioned start must have produced an instance (never 0) — require its
            // presence, in addition to the per-step exactly-one-version / highest-version checks.
            Invariants.assertVersionRoutingSound(world.committedLog(), VERSIONED_ID_PREFIX,
                                                 VersionedOrderWorkflow.VERSION_V2, Set.of(VERSIONED_WORKFLOW_ID));
            // INV-12 at the horizon: the migrating instance must have completed, recorded its migration marker exactly
            // once, and taken the migrated branch (a fresh start migrates forward, so processV2 is present and the
            // legacy chargeV1 is absent). The at-most-once / monotonic checks ran every step; here we also pin that the
            // marker is actually present and the migrated branch ran.
            Invariants.assertMigrateVersionContract(world.committedLog(), MIGRATING_ID_PREFIX);
            assertMigratingInstanceMigratedExactlyOnce(world);
            // INV-13 at the horizon: the payload instance must have completed and its engine-reconstructed final payload
            // must carry ALL FOUR distinct keys its steps wrote — so the per-step no-lost-write check ran against a real
            // multi-write payload (not vacuously satisfied by an instance that never wrote) and every committed
            // contribution actually survived to the final payload. The per-step assertion already enforced no-lost-write
            // after every step (incl. across the crash/restart/reorder faults); here we pin the end state is complete.
            Invariants.assertNoLostPayloadWrites(world.committedLog(), PAYLOAD_ID_PREFIX,
                                                 world.engine().reconstructedPayloads(PAYLOAD_ID_PREFIX));
            assertPayloadInstanceWroteAllKeys(world);
            // INV-14 at the horizon: the combinator instance must have completed and recorded each combinator's decision
            // consistently with its committed branch outcomes. The per-step assertion already ran after every step (incl.
            // across the crash/restart/reorder faults); here we also pin that the expected decisions for this workflow's
            // fixed branch design (A=yes, B=no, C=no) were actually recorded — so the per-step check ran against a real
            // resolved combinator (not vacuously satisfied by an instance that never reached the combinators).
            Invariants.assertCombinatorConsistency(world.committedLog(), COMBINATOR_ID_PREFIX,
                                                   world.engine().reconstructedPayloads(COMBINATOR_ID_PREFIX));
            assertCombinatorInstanceRecordedExpectedDecisions(world);
            // INV-15 at the horizon: both correlated-wait instances must have completed their wait, each matching on its
            // OWN key — so the per-step exact-correlation check ran against a real completed wait (not vacuously
            // satisfied by an instance that never woke) and neither was woken by the other's signal. The per-step
            // assertion already enforced no-cross-wakeup / at-most-once after every step (incl. across the
            // crash/restart/reorder/duplicate faults); here we pin the end state is complete and correctly matched.
            Invariants.assertEventCorrelationExact(world.committedLog(), CORRELATED_ID_PREFIX,
                                                   world.engine().reconstructedPayloads(CORRELATED_ID_PREFIX));
            assertCorrelatedInstancesMatchedOwnKey(world);
            // INV-19 at the horizon: the reducer instance must have completed and its engine-reconstructed payload must
            // match the documented reducer fold exactly — and that final payload must demonstrate each reducer's
            // documented outcome (combine keys present, the global_only result key absent, the local_only replace having
            // dropped a prior key, the parameter-side combine view recorded true) — so the per-step check ran against a
            // real multi-reducer payload (not vacuously satisfied by an instance that never wrote). The per-step
            // assertion already enforced reducer semantics after every step (incl. across the crash/restart/reorder
            // faults); here we pin the end state demonstrates every reducer. The fold-equals cross-check derives the
            // expected fold (A) in the deterministic logical step order (so a post-RESTART re-append interleaving cannot
            // flake it); the authoritative end-state completeness is enforced by assertReducerInstanceDemonstratedEveryReducer
            // below, which reads the reconstructed payload directly.
            Invariants.assertPayloadReducerSemantics(world.committedLog(), REDUCER_ID_PREFIX,
                                                     world.engine().reconstructedPayloads(REDUCER_ID_PREFIX));
            assertReducerInstanceDemonstratedEveryReducer(world);
            // INV-20 at the horizon: now the versioning-edges start must have produced an instance (never 0) — require
            // its presence, in addition to the per-step no-recorded-downgrade / at-most-once-per-changeId /
            // monotonic / exactly-one-highest-version checks.
            Invariants.assertVersioningEdges(world.committedLog(), VERSIONING_EDGES_ID_PREFIX,
                                             VersioningEdgesWorkflow.VERSION_HIGH,
                                             VersioningEdgesWorkflow.CHANGE_ID_DOWNGRADE, true,
                                             Set.of(VERSIONING_EDGES_WORKFLOW_ID));
            assertVersioningEdgesInstanceExercisedEveryEdge(world);
            // INV-22 at the horizon: now the custom-named start must have produced an instance that reached a terminal
            // status (never routed to none, completed under customization) — require its presence + terminal, in
            // addition to the per-step customized-name-applied checks. Then pin that the instance actually recorded the
            // expected customized step events (a customized STARTED + the overridden COMPLETED "Done" suffix for each
            // step), so the per-step check ran against a real customized instance rather than being vacuously satisfied.
            Invariants.assertEventNameCustomizationSound(world.committedLog(), CUSTOM_NAMED_ID_PREFIX,
                                                         Set.of(CUSTOM_NAMED_WORKFLOW_ID));
            assertCustomNamedInstanceRecordedCustomizedNames(world);
            // INV-6: document the F-0 gap as an expected outcome. Every started workflow's reserveInventory effect
            // must have run at least once; if any effect ran twice it is the documented F-0 duplication.
            for (String orderId : orderIds) {
                String workflowId = "order-" + orderId;
                Invariants.documentEffectAtMostOnceGap(
                        workflowId, OrderWorkflow.STEP_RESERVE_INVENTORY,
                        world.effects().count(workflowId, OrderWorkflow.STEP_RESERVE_INVENTORY), false);
            }
        } catch (InvariantViolation violation) {
            throw enrich(violation, world, context);
        }
    }

    /**
     * Horizon pin for INV-12 ({@code MigrateVersionContract}): the single migrating instance must have actually
     * recorded its {@code migrateVersion} marker exactly once (so the per-step at-most-once / monotonic checks were not
     * vacuously satisfied by an instance that never migrated) and taken the migrated branch — a fresh start migrates
     * forward, so its {@code processV2} step is present and the legacy {@code chargeV1} step is absent. Reads the
     * committed log directly so it is authoritative; a deviation throws an {@link InvariantViolation} (routed through
     * {@code enrich} by the caller) rather than passing silently.
     */
    private void assertMigratingInstanceMigratedExactlyOnce(SimulationWorld world) {
        var committedLog = world.committedLog();
        long markerCount = committedLog.stream()
                .filter(e -> MIGRATING_WORKFLOW_ID.equals(MetadataUtils.getWorkflowId(e.metadata())))
                .filter(e -> MetadataUtils.isVersionMigrationStep(e.metadata()))
                .filter(e -> MigratingOrderWorkflow.CHANGE_ID.equals(
                        MetadataUtils.getVersionChangeId(e.metadata()).orElse(null)))
                .count();
        if (markerCount != 1) {
            throw new InvariantViolation(
                    "MigrateVersionContract",
                    "migrating instance '" + MIGRATING_WORKFLOW_ID + "' recorded its migrateVersion('"
                            + MigratingOrderWorkflow.CHANGE_ID + "') marker " + markerCount + " time(s) by the horizon — "
                            + "a fresh start must record it exactly once");
        }
        boolean migratedBranch = hasStepInLog(committedLog, MIGRATING_WORKFLOW_ID, MigratingOrderWorkflow.STEP_PROCESS_V2);
        boolean legacyBranch = hasStepInLog(committedLog, MIGRATING_WORKFLOW_ID, MigratingOrderWorkflow.STEP_CHARGE_V1);
        if (!migratedBranch || legacyBranch) {
            throw new InvariantViolation(
                    "MigrateVersionContract",
                    "migrating instance '" + MIGRATING_WORKFLOW_ID + "' must take the migrated branch on a fresh start ("
                            + MigratingOrderWorkflow.STEP_PROCESS_V2 + " present=" + migratedBranch + ", legacy "
                            + MigratingOrderWorkflow.STEP_CHARGE_V1 + " present=" + legacyBranch + ")");
        }
    }

    /**
     * Horizon pin for INV-20 ({@code VersioningEdges}): the single versioning-edges instance must have actually
     * exercised every edge by the horizon, so the per-step checks were not vacuously satisfied by an instance that never
     * migrated. It must have recorded BOTH forward-migration markers (one per distinct {@code changeId}) exactly once,
     * recorded the observable {@code downgradeRejected} step (so the downgrade migration was genuinely attempted and
     * rejected), and recorded NO marker for the downgrade {@code changeId}. Reads the committed log directly so it is
     * authoritative; a deviation throws an {@link InvariantViolation} (routed through {@code enrich} by the caller).
     */
    private void assertVersioningEdgesInstanceExercisedEveryEdge(SimulationWorld world) {
        var committedLog = world.committedLog();
        long bump1 = migrationMarkerCount(committedLog, VersioningEdgesWorkflow.CHANGE_ID_1);
        long bump2 = migrationMarkerCount(committedLog, VersioningEdgesWorkflow.CHANGE_ID_2);
        long downgrade = migrationMarkerCount(committedLog, VersioningEdgesWorkflow.CHANGE_ID_DOWNGRADE);
        boolean rejectedStep = hasStepInLog(committedLog, VERSIONING_EDGES_WORKFLOW_ID,
                                            VersioningEdgesWorkflow.STEP_DOWNGRADE_REJECTED);
        if (bump1 != 1 || bump2 != 1 || downgrade != 0 || !rejectedStep) {
            throw new InvariantViolation(
                    "VersioningEdges",
                    "versioning-edges instance '" + VERSIONING_EDGES_WORKFLOW_ID + "' did not exercise every edge by the "
                            + "horizon: forward markers '" + VersioningEdgesWorkflow.CHANGE_ID_1 + "'=" + bump1 + " '"
                            + VersioningEdgesWorkflow.CHANGE_ID_2 + "'=" + bump2 + " (each must be 1), downgrade marker '"
                            + VersioningEdgesWorkflow.CHANGE_ID_DOWNGRADE + "'=" + downgrade + " (must be 0 — rejected, "
                            + "never recorded), downgradeRejected step present=" + rejectedStep + " (must be true — the "
                            + "rejection was genuinely exercised)");
        }
    }

    /**
     * Counts the committed migration markers for the versioning-edges instance carrying the given {@code changeId}.
     */
    private static long migrationMarkerCount(List<EventMessage> committedLog, String changeId) {
        return committedLog.stream()
                .filter(e -> VERSIONING_EDGES_WORKFLOW_ID.equals(MetadataUtils.getWorkflowId(e.metadata())))
                .filter(e -> MetadataUtils.isVersionMigrationStep(e.metadata()))
                .filter(e -> changeId.equals(MetadataUtils.getVersionChangeId(e.metadata()).orElse(null)))
                .count();
    }

    /**
     * Horizon pin for INV-22 ({@code EventNameCustomizationSound}): the single custom-named instance must have actually
     * recorded its expected customized step/status events by the horizon, so the per-step customized-name check was not
     * vacuously satisfied by an instance that never ran. It must carry — under the custom namespace — a customized
     * STARTED and the overridden {@code Done}-suffixed COMPLETED for each of its two steps, and the customized workflow
     * COMPLETED status event ({@code CustomNamedCompleted}). Reads the committed log directly so it is authoritative; a
     * missing expected customized name throws an {@link InvariantViolation} (routed through {@code enrich} by the
     * caller) rather than passing silently.
     */
    private void assertCustomNamedInstanceRecordedCustomizedNames(SimulationWorld world) {
        var committedLog = world.committedLog();
        var expected = List.of(
                CustomNamedWorkflow.expectedStepLocalName(CustomNamedWorkflow.STEP_PREPARE_ORDER,
                                                          CustomNamedWorkflow.STEP_STARTED_SUFFIX),
                CustomNamedWorkflow.expectedStepLocalName(CustomNamedWorkflow.STEP_PREPARE_ORDER,
                                                          CustomNamedWorkflow.STEP_COMPLETED_SUFFIX),
                CustomNamedWorkflow.expectedStepLocalName(CustomNamedWorkflow.STEP_FINALIZE_ORDER,
                                                          CustomNamedWorkflow.STEP_STARTED_SUFFIX),
                CustomNamedWorkflow.expectedStepLocalName(CustomNamedWorkflow.STEP_FINALIZE_ORDER,
                                                          CustomNamedWorkflow.STEP_COMPLETED_SUFFIX),
                CustomNamedWorkflow.expectedWorkflowLocalName(CustomNamedWorkflow.WORKFLOW_COMPLETED_SUFFIX));
        for (String localName : expected) {
            boolean present = committedLog.stream().anyMatch(e ->
                    CUSTOM_NAMED_WORKFLOW_ID.equals(MetadataUtils.getWorkflowId(e.metadata()))
                            && CustomNamedWorkflow.NAMESPACE.equals(e.type().qualifiedName().namespace())
                            && localName.equals(e.type().qualifiedName().localName()));
            if (!present) {
                throw new InvariantViolation(
                        "EventNameCustomizationSound",
                        "custom-named instance '" + CUSTOM_NAMED_WORKFLOW_ID + "' did not record the expected customized "
                                + "event '" + CustomNamedWorkflow.NAMESPACE + "/" + localName + "' by the horizon — the "
                                + "registered eventNameCustomizer was not applied (or the instance never ran the step)");
            }
        }
    }

    /**
     * Horizon pin for INV-13 ({@code NoLostPayloadWrites}): the single payload instance must have actually recorded all
     * four distinct payload keys its steps write, in <strong>both</strong> reconstructions — the engine's own
     * reconstructed payload (the history read-model) <em>and</em> the committed-log fold ({@link Invariants#rebuildPayload})
     * — so the per-step no-lost-write check was exercised against a real multi-write payload rather than vacuously
     * satisfied by an instance that never wrote, and the two independent reconstructions agree at the end. Reads the
     * committed log + read-model directly so it is authoritative; a missing key throws an {@link InvariantViolation}
     * (routed through {@code enrich} by the caller) rather than passing silently.
     */
    private void assertPayloadInstanceWroteAllKeys(SimulationWorld world) {
        var loggedPayload = Invariants.rebuildPayload(world.committedLog(), PAYLOAD_WORKFLOW_ID);
        var actualPayload = world.engine().reconstructedPayloads(PAYLOAD_ID_PREFIX)
                                 .getOrDefault(PAYLOAD_WORKFLOW_ID, Map.of());
        var expectedKeys = List.of(PayloadOrderWorkflow.KEY_INVENTORY_RESERVED,
                                   PayloadOrderWorkflow.KEY_PAYMENT_CHARGED,
                                   PayloadOrderWorkflow.KEY_SHIPMENT_RECORDED,
                                   PayloadOrderWorkflow.KEY_ORDER_FINALIZED);
        for (String key : expectedKeys) {
            if (!loggedPayload.containsKey(key) || !actualPayload.containsKey(key)) {
                throw new InvariantViolation(
                        "NoLostPayloadWrites",
                        "payload instance '" + PAYLOAD_WORKFLOW_ID + "' is missing the '" + key + "' key its step wrote "
                                + "by the horizon (committed-log payload=" + loggedPayload + ", engine-reconstructed "
                                + "payload=" + actualPayload + ") — a committed payload write is absent from the final "
                                + "payload");
            }
        }
    }

    /**
     * Horizon pin for INV-19 ({@code PayloadReducerSemantics}): the single reducer instance must have completed and its
     * engine-reconstructed final payload must demonstrate each reducer's documented outcome — so the per-step check ran
     * against a real multi-reducer payload rather than being vacuously satisfied by an instance that never wrote:
     * <ul>
     *   <li>the {@code combine} key ({@link ReducerWorkflow#KEY_COMBINE}) is PRESENT with its value (combine merged);</li>
     *   <li>the {@code global_only} result key ({@link ReducerWorkflow#KEY_GLOBAL_ONLY}) is ABSENT (the default
     *       {@code global_only} result reducer discarded it);</li>
     *   <li>the {@code local_only}-dropped prior key ({@link ReducerWorkflow#KEY_SEED}) is ABSENT (the {@code modifyPayload}
     *       replace dropped it) while the replacement key ({@link ReducerWorkflow#KEY_REPLACE}) survives;</li>
     *   <li>the parameter-side combine view ({@link ReducerWorkflow#KEY_PARAM_SAW_COMBINE}) is {@code true} (the
     *       {@code parameterPayloadReducer(combine)} step SAW the combined global payload);</li>
     *   <li>edge (b): the interplay key ({@link ReducerWorkflow#KEY_INTERPLAY}) is ABSENT (a combine parameter view does
     *       NOT make a {@code global_only} result write land — the two reducers are independent knobs);</li>
     *   <li>edge (c): the last-writer-wins key ({@link ReducerWorkflow#KEY_LWW}) carries the LATER writer's value
     *       ({@link ReducerWorkflow#VALUE_LWW_SECOND}) — the later same-key combine write wins.</li>
     * </ul>
     * Reads the engine's reconstructed payload directly so it is authoritative; a deviation throws an
     * {@link InvariantViolation} (routed through {@code enrich} by the caller) rather than passing silently.
     */
    private void assertReducerInstanceDemonstratedEveryReducer(SimulationWorld world) {
        var payload = world.engine().reconstructedPayloads(REDUCER_ID_PREFIX)
                           .getOrDefault(REDUCER_WORKFLOW_ID, Map.of());
        // combine merged: the combine key is present with its value.
        if (!ReducerWorkflow.VALUE_COMBINE.equals(payload.get(ReducerWorkflow.KEY_COMBINE))) {
            throw new InvariantViolation(
                    "PayloadReducerSemantics",
                    "reducer instance '" + REDUCER_WORKFLOW_ID + "' final payload must carry the combine key '"
                            + ReducerWorkflow.KEY_COMBINE + "'=" + ReducerWorkflow.VALUE_COMBINE
                            + " by the horizon (combine merges its result) — payload=" + payload);
        }
        // global_only discarded: the global_only result key must be absent.
        if (payload.containsKey(ReducerWorkflow.KEY_GLOBAL_ONLY)) {
            throw new InvariantViolation(
                    "PayloadReducerSemantics",
                    "reducer instance '" + REDUCER_WORKFLOW_ID + "' final payload must NOT carry the global_only result "
                            + "key '" + ReducerWorkflow.KEY_GLOBAL_ONLY + "' (the default global_only reducer discards "
                            + "the result) — payload=" + payload);
        }
        // local_only replace dropped the prior seed key but kept the replacement key.
        if (payload.containsKey(ReducerWorkflow.KEY_SEED)
                || !ReducerWorkflow.VALUE_REPLACE.equals(payload.get(ReducerWorkflow.KEY_REPLACE))) {
            throw new InvariantViolation(
                    "PayloadReducerSemantics",
                    "reducer instance '" + REDUCER_WORKFLOW_ID + "' final payload must reflect the local_only replace: "
                            + "the prior key '" + ReducerWorkflow.KEY_SEED + "' dropped and the replacement key '"
                            + ReducerWorkflow.KEY_REPLACE + "'=" + ReducerWorkflow.VALUE_REPLACE + " present — payload="
                            + payload);
        }
        // parameter-side combine view: the action saw the combined global payload.
        if (!Boolean.TRUE.equals(payload.get(ReducerWorkflow.KEY_PARAM_SAW_COMBINE))) {
            throw new InvariantViolation(
                    "PayloadReducerSemantics",
                    "reducer instance '" + REDUCER_WORKFLOW_ID + "' final payload must record the parameter-side combine "
                            + "view '" + ReducerWorkflow.KEY_PARAM_SAW_COMBINE + "'=true (a parameterPayloadReducer("
                            + "combine) step sees the global payload) — payload=" + payload);
        }
        // edge (b) parameter-view vs result-write are independent: the interplay step's combine PARAMETER view did not
        // make its global_only RESULT write land — its key must be absent (the write-back was discarded).
        if (payload.containsKey(ReducerWorkflow.KEY_INTERPLAY)) {
            throw new InvariantViolation(
                    "PayloadReducerSemantics",
                    "reducer instance '" + REDUCER_WORKFLOW_ID + "' final payload must NOT carry the interplay result key '"
                            + ReducerWorkflow.KEY_INTERPLAY + "' — a combine parameter view does NOT imply a combine "
                            + "write-back; its global_only result reducer discards the result — payload=" + payload);
        }
        // edge (c) last-writer-wins on the same key: the later same-key combine write wins, so the key carries the
        // LATER writer's value (never the first writer's).
        if (!ReducerWorkflow.VALUE_LWW_SECOND.equals(payload.get(ReducerWorkflow.KEY_LWW))) {
            throw new InvariantViolation(
                    "PayloadReducerSemantics",
                    "reducer instance '" + REDUCER_WORKFLOW_ID + "' final payload must carry the last-writer-wins key '"
                            + ReducerWorkflow.KEY_LWW + "'=" + ReducerWorkflow.VALUE_LWW_SECOND + " (the later same-key "
                            + "combine write wins) — payload=" + payload);
        }
    }

    /**
     * Horizon pin for INV-14 ({@code CombinatorConsistency}): the single combinator instance must have actually reached
     * the combinators and recorded the decisions this workflow's fixed branch design dictates — {@code anyMatch} matched
     * (branch A votes yes), {@code allMatch} unmatched (B/C vote no), {@code noneMatch} unmatched (A votes yes, so a
     * match exists) — so the per-step consistency check ran against a real resolved combinator rather than being
     * vacuously satisfied by an instance that never reached them. Reads the committed log directly so it is
     * authoritative; a deviation throws an {@link InvariantViolation} (routed through {@code enrich} by the caller)
     * rather than passing silently.
     */
    private void assertCombinatorInstanceRecordedExpectedDecisions(SimulationWorld world) {
        var committedLog = world.committedLog();
        // The fixed branch design (A=yes, B=no, C=no) under the VOTED_YES predicate fixes every decision:
        //   anyMatch -> matched, allMatch -> unmatched, noneMatch -> unmatched.
        record Pin(String matchedStep, String unmatchedStep, boolean expectedMatched) {

        }
        var pins = List.of(
                new Pin(CombinatorWorkflow.STEP_ANY_MATCHED, CombinatorWorkflow.STEP_ANY_UNMATCHED, true),
                new Pin(CombinatorWorkflow.STEP_ALL_MATCHED, CombinatorWorkflow.STEP_ALL_UNMATCHED, false),
                new Pin(CombinatorWorkflow.STEP_NONE_MATCHED, CombinatorWorkflow.STEP_NONE_UNMATCHED, false));
        for (Pin pin : pins) {
            String expectedStep = pin.expectedMatched() ? pin.matchedStep() : pin.unmatchedStep();
            String forbiddenStep = pin.expectedMatched() ? pin.unmatchedStep() : pin.matchedStep();
            if (!hasStepInLog(committedLog, COMBINATOR_WORKFLOW_ID, expectedStep)
                    || hasStepInLog(committedLog, COMBINATOR_WORKFLOW_ID, forbiddenStep)) {
                throw new InvariantViolation(
                        "CombinatorConsistency",
                        "combinator instance '" + COMBINATOR_WORKFLOW_ID + "' must record the post-combinator step '"
                                + expectedStep + "' (and not '" + forbiddenStep + "') by the horizon for its fixed branch "
                                + "design (A=yes, B=no, C=no) — the combinator did not resolve the documented decision");
            }
        }
    }

    /**
     * Horizon pin for INV-15 ({@code EventCorrelationExact}): both correlated-wait instances must have actually
     * completed their wait and recorded a {@code matchedKey} equal to their OWN correlation key — so the per-step
     * exact-correlation check ran against a real completed wait (not vacuously satisfied by an instance that never woke)
     * and neither was woken by the other's signal (no cross-wakeup). Reads the engine's reconstructed payload + the
     * committed log directly so it is authoritative; a missing/foreign matched key throws an {@link InvariantViolation}
     * (routed through {@code enrich} by the caller) rather than passing silently.
     */
    private void assertCorrelatedInstancesMatchedOwnKey(SimulationWorld world) {
        var payloads = world.engine().reconstructedPayloads(CORRELATED_ID_PREFIX);
        assertCorrelatedInstanceMatched(CORRELATED_WORKFLOW_ID_A, CORRELATED_KEY_A, payloads);
        assertCorrelatedInstanceMatched(CORRELATED_WORKFLOW_ID_B, CORRELATED_KEY_B, payloads);
    }

    /**
     * Pins one correlated-wait instance: its engine-reconstructed payload must record {@code matchedKey} equal to its
     * own correlation key (it woke on its own signal, not a foreign one).
     */
    private void assertCorrelatedInstanceMatched(String workflowId, String ownKey,
                                                 Map<String, Map<String, Object>> payloads) {
        Object matched = payloads.getOrDefault(workflowId, Map.of())
                                 .get(CorrelatedWaitWorkflow.KEY_MATCHED_KEY);
        if (!ownKey.equals(String.valueOf(matched))) {
            throw new InvariantViolation(
                    "EventCorrelationExact",
                    "correlated-wait instance '" + workflowId + "' (own key '" + ownKey + "') has engine-reconstructed "
                            + "matchedKey '" + matched + "' by the horizon — it must have completed its wait on its OWN "
                            + "key (a missing match = never woken; a foreign key = cross-wakeup)");
        }
    }

    /**
     * {@code true} iff the committed log holds a step event with {@code stepName} for {@code workflowId}.
     */
    private static boolean hasStepInLog(List<EventMessage> committedLog, String workflowId,
                                        String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).isPresent()
                        && stepName.equals(MetadataUtils.getStepName(e.metadata())));
    }

    /**
     * Returns the order ids whose committed log does <em>not</em> contain a terminal workflow-status event — i.e. the
     * instances that genuinely did not terminate. Reads the durable committed log, which is authoritative and not
     * subject to the async drain of the in-memory live set.
     */
        private List<String> nonTerminalOrders(SimulationWorld world, List<String> orderIds) {
        var committedLog = world.committedLog();
        var nonTerminal = new ArrayList<String>();
        for (String orderId : orderIds) {
            String workflowId = "order-" + orderId;
            if (!isTerminalInLog(committedLog, workflowId)) {
                nonTerminal.add(workflowId);
            }
        }
        return nonTerminal;
    }

    /**
     * Returns the non-terminal workflow ids across the order instances, the single versioned instance, the single
     * migrating instance, the single payload instance, the single combinator instance, the two correlated-wait instances
     * and the single reducer instance — the full set the loop drives to termination. The versioned
     * ({@link #VERSIONED_WORKFLOW_ID}), migrating ({@link #MIGRATING_WORKFLOW_ID}), payload ({@link #PAYLOAD_WORKFLOW_ID}),
     * combinator ({@link #COMBINATOR_WORKFLOW_ID}), reducer ({@link #REDUCER_WORKFLOW_ID}) and custom-named
     * ({@link #CUSTOM_NAMED_WORKFLOW_ID}) instances self-complete via execute/modifyPayload steps; the correlated-wait
     * instances ({@link #CORRELATED_WORKFLOW_ID_A}/{@link #CORRELATED_WORKFLOW_ID_B}) complete once their own key's signal
     * is delivered (the harness delivers it every step + at the horizon — at-least-once). Including them all makes a
     * stuck instance a liveness failure (a routing/replay, migration-record, payload-write, combinator-decision,
     * correlation, reducer-semantics, or event-name-customization regression) rather than a silent pass.
     */
        private List<String> allNonTerminal(SimulationWorld world, List<String> orderIds) {
        var nonTerminal = new ArrayList<>(nonTerminalOrders(world, orderIds));
        if (!isTerminalInLog(world.committedLog(), VERSIONED_WORKFLOW_ID)) {
            nonTerminal.add(VERSIONED_WORKFLOW_ID);
        }
        if (!isTerminalInLog(world.committedLog(), MIGRATING_WORKFLOW_ID)) {
            nonTerminal.add(MIGRATING_WORKFLOW_ID);
        }
        if (!isTerminalInLog(world.committedLog(), PAYLOAD_WORKFLOW_ID)) {
            nonTerminal.add(PAYLOAD_WORKFLOW_ID);
        }
        if (!isTerminalInLog(world.committedLog(), COMBINATOR_WORKFLOW_ID)) {
            nonTerminal.add(COMBINATOR_WORKFLOW_ID);
        }
        if (!isTerminalInLog(world.committedLog(), CORRELATED_WORKFLOW_ID_A)) {
            nonTerminal.add(CORRELATED_WORKFLOW_ID_A);
        }
        if (!isTerminalInLog(world.committedLog(), CORRELATED_WORKFLOW_ID_B)) {
            nonTerminal.add(CORRELATED_WORKFLOW_ID_B);
        }
        if (!isTerminalInLog(world.committedLog(), REDUCER_WORKFLOW_ID)) {
            nonTerminal.add(REDUCER_WORKFLOW_ID);
        }
        if (!isTerminalInLog(world.committedLog(), CUSTOM_NAMED_WORKFLOW_ID)) {
            nonTerminal.add(CUSTOM_NAMED_WORKFLOW_ID);
        }
        return nonTerminal;
    }

    /**
     * {@code true} iff the committed log holds a terminal workflow-status event for {@code workflowId}.
     */
    private static boolean isTerminalInLog(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }

    /**
     * Returns the order ids (un-prefixed) of instances that have not yet reached a terminal workflow status.
     */
        private List<String> nonTerminalOrderIds(SimulationWorld world, List<String> orderIds) {
        var nonTerminalWf = nonTerminalOrders(world, orderIds);
        var result = new ArrayList<String>();
        for (String orderId : orderIds) {
            if (nonTerminalWf.contains("order-" + orderId)) {
                result.add(orderId);
            }
        }
        return result;
    }


    /**
     * Waits for the asynchronous processor/body threads to catch up to a stable point: the committed log has stopped
     * growing for a sustained window of consecutive polls. The only async elements are the Axon processor thread and
     * the per-instance body virtual threads (the clock and the timer scheduler are manual and advanced explicitly by
     * the loop), so once the log is stable the engine has finished routing everything delivered so far. The state the
     * harness then inspects is the deterministic result of the events delivered and timers fired up to this point.
     *
     * @return {@code true} iff genuine quiescence was reached within the (deadline-clamped) per-settle budget — the
     *         committed log stayed stable for the full quiet window with pending timers drained or established
     *         never-due; {@code false} when the budget elapsed with work still visibly in flight (the load-lag case
     *         the settle-aware step counter must not charge against the max-steps cap).
     */
    private boolean settle(SimulationWorld world, String label, SimulationContext context,
                           Deadline deadline) {
        // Drive to a true quiescent point: as long as the engine is producing events OR has pending durable-delay
        // timers (a parked sleep / retry backoff), keep nudging virtual time forward so those timers fire, then wait
        // for the committed log to stop changing for a continuous wall-clock window. The wall-clock window is robust
        // under CI CPU contention (a transient pause between two body steps is shorter than the window, while a
        // caught-up engine stays unchanged for its whole duration). The wait is bounded by a SHORT per-settle budget,
        // itself clamped to whatever remains of the global deadline, so this can never block past the hard stop.
        final long[] lastChangeNanos = {System.nanoTime()};
        final int[] lastCommitted = {world.committedLog().size()};
        final int[] nudges = {0};
        boolean settled = Polling.await(deadline.clamp(SETTLE_BUDGET), () -> {
            // Fire pending durable-delay timers (a parked sleep/backoff) by nudging virtual time, but only up to a cap
            // so a still-pending long wait timeout cannot make settle spin forever.
            if (world.scheduler().pendingTasks() > 0 && nudges[0] < MAX_NUDGES_PER_SETTLE) {
                world.advanceTime(SETTLE_TIME_NUDGE);
                nudges[0]++;
                lastChangeNanos[0] = System.nanoTime();
                lastCommitted[0] = world.committedLog().size();
                return false;
            }
            int committed = world.committedLog().size();
            if (committed != lastCommitted[0]) {
                lastCommitted[0] = committed;
                lastChangeNanos[0] = System.nanoTime();
                return false;
            }
            return System.nanoTime() - lastChangeNanos[0] >= SETTLE_QUIET_WINDOW.toNanos();
        });
        if (!settled) {
            // Not settling within the budget while the global deadline has expired is a hang — abort with the full
            // diagnostic rather than letting the next wait block. If the budget elapsed but the global deadline still
            // has room (e.g. a transient CI pause), log and continue; the loop's next checkNotExpired will catch a
            // genuine stall, and a non-quiescent state is still soundly inspected (it can only have fewer events than
            // a fully-settled one, never a torn write).
            if (deadline.isExpired()) {
                throw abort("EventuallyTerminates",
                            "did not settle at '" + label + "' before the wall-clock deadline (live="
                                    + world.engine().liveWorkflowIds() + ", committed=" + world.committedLog().size()
                                    + ", pendingTimers=" + world.scheduler().pendingTasks() + ")",
                            world, context);
            }
            logger.warn("seed={} did not fully settle at '{}' within the per-settle budget (live={}, committed={}, "
                                + "pendingTimers={}); deadline not yet reached, continuing",
                        config.seed(), label, world.engine().liveWorkflowIds(), world.committedLog().size(),
                        world.scheduler().pendingTasks());
        }
        return settled;
    }

    /**
     * Builds a fresh enriched {@link InvariantViolation} for a harness-level abort (a wall-clock-deadline expiry or a
     * max-steps overrun). Routed through the same enrichment as a genuine invariant break so the seed, fault trace,
     * committed log and reproduce command are always attached.
     */
        private InvariantViolation abort(String machineName, String reason,
                                     SimulationWorld world, SimulationContext context) {
        return enrich(new InvariantViolation(machineName, "HARNESS ABORT: " + reason), world, context);
    }

        private InvariantViolation enrich(
            InvariantViolation violation,
            SimulationWorld world, SimulationContext context) {
        var message = new StringBuilder(violation.getMessage())
                .append("\n  seed=").append(config.seed())
                .append("\n  reproduce: ./mvnw -pl workflow/axoniq-workflow-simulation -am test -Dtest=DstReproduceTest -Ddst.seed=")
                .append(config.seed())
                .append("\n  fault trace:");
        for (String line : context.trace()) {
            message.append("\n    ").append(line);
        }
        message.append("\n  committed log:");
        for (String line : world.eventStore().renderCommittedLog()) {
            message.append("\n    ").append(line);
        }
        logger.error("INVARIANT VIOLATION (seed={}):\n{}", config.seed(), message);
        return new InvariantViolation(violation.machineName(),
                                                                              message.toString());
    }

    /**
     * The PRIMARY anti-hang mechanism: a hard real-time deadline for one {@link #run()}. The seeded loop checks it
     * every iteration ({@link #checkNotExpired}), and every bounded wait clamps its budget to whatever remains
     * ({@link #clamp}), so no wait can ever outlive the deadline and the harness is structurally incapable of hanging.
     * On expiry the loop throws an enriched diagnostic instead of blocking.
     */
    private final class Deadline {

        private final long deadlineNanos;

        private Deadline(Duration budget) {
            this.deadlineNanos = System.nanoTime() + budget.toNanos();
        }

        /**
         * Returns the time remaining before the deadline, never negative (zero once expired).
         */
                private Duration remaining() {
            long remaining = deadlineNanos - System.nanoTime();
            return remaining <= 0 ? Duration.ZERO : Duration.ofNanos(remaining);
        }

        /**
         * Returns {@code true} once the deadline has passed.
         */
        private boolean isExpired() {
            return System.nanoTime() >= deadlineNanos;
        }

        /**
         * Clamps a per-wait budget to whatever remains of the deadline, so a single wait can never block past the hard
         * stop. Returns a tiny non-zero floor when essentially expired so a final non-blocking poll still happens.
         */
                private Duration clamp(Duration budget) {
            Duration remaining = remaining();
            if (remaining.isZero()) {
                return Duration.ofMillis(1);
            }
            return remaining.compareTo(budget) < 0 ? remaining : budget;
        }

        /**
         * Throws an enriched diagnostic if the deadline has expired, aborting the run; otherwise returns.
         *
         * @param where a short label for where in the run the check fired.
         */
        private void checkNotExpired(SimulationWorld world, SimulationContext context,
                                     String where) {
            if (isExpired()) {
                throw abort("EventuallyTerminates",
                            "wall-clock deadline of " + config.wallClockDeadline() + " exceeded at " + where
                                    + " — aborting (potential hang/liveness finding); live="
                                    + world.engine().liveWorkflowIds(),
                            world, context);
            }
        }
    }

    /**
     * Renders the committed log as a per-workflow fingerprint: events grouped by {@code workflowId} (groups sorted by
     * id), each group keeping its own event order. This is the meaningful notion of determinism for INV-4 — each
     * instance's state is a pure function of its own history — and it factors out the inherently concurrent, F-2
     * cross-instance interleaving of the single global log (ARCHITECTURE.md §8/§11), which is order-independent
     * because distinct instances are independent.
     *
     * @param committedLog the committed workflow event log.
     * @return per-workflow ordered fingerprint lines.
     */
        private static List<String> fingerprint(List<EventMessage> committedLog) {
        var lines = new ArrayList<String>();
        // Render each instance's events as a canonically-sorted MULTISET, not in global-append order: the per-instance
        // global-append order is the F-2 non-deterministic surface (cross-instance, and intra-instance for events
        // committed close together — the engine publishes durably-async, so a step's terminal record can land just
        // before/after the workflow-completion commit run-to-run). Sorting makes the seed-reproducibility fingerprint
        // (DstReproduceTest / DstSmokeTest.sameSeedIsReproducible) compare per-instance CONTENT, which is deterministic,
        // rather than that order. The append-order view (perWorkflowEvents) is kept for the intra-run prefix-stability
        // check, which is sound because within one run the committed log only grows (append-only, never reordered).
        perWorkflowEvents(committedLog).forEach((workflowId, events) -> {
            var sorted = new ArrayList<>(events);
            java.util.Collections.sort(sorted);
            lines.add(workflowId + " => " + sorted);
        });
        return List.copyOf(lines);
    }

    /**
     * Groups the committed log by {@code workflowId} (sorted), each group preserving its own event order, rendered as
     * {@code stepName:status} strings. This is the per-instance subsequence used for both intra-run prefix stability
     * and run-level seed equality.
     *
     * @param committedLog the committed workflow event log.
     * @return ordered per-workflow event subsequences.
     */
        private static Map<String, List<String>> perWorkflowEvents(List<EventMessage> committedLog) {
        var byWorkflow = new java.util.TreeMap<String, List<String>>();
        for (EventMessage event : committedLog) {
            var workflowId = MetadataUtils.getWorkflowId(event.metadata());
            var status = MetadataUtils.getStepStatus(event.metadata()).map(Enum::name)
                    .or(() -> MetadataUtils.getWorkflowStatus(event.metadata()).map(Enum::name))
                    .orElse("-");
            var stepName = MetadataUtils.getStepStatus(event.metadata()).isPresent()
                    ? MetadataUtils.getStepName(event.metadata())
                    : "<workflow>";
            byWorkflow.computeIfAbsent(workflowId, k -> new ArrayList<>()).add(stepName + ":" + status);
        }
        return byWorkflow;
    }
}
