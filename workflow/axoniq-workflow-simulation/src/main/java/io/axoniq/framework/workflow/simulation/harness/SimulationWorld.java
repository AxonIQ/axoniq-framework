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

import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.runtime.test.fakes.ManualWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.fakes.MutableClock;
import io.axoniq.framework.workflow.runtime.test.fakes.SeededWorkflowIdGenerator;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.common.ClockUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.time.Clock;
import java.time.Duration;
import org.axonframework.common.configuration.ComponentRegistry;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The simulated world: the durable substrate (event store, safe-point store, history read-model) plus the
 * deterministic seams (virtual-time scheduler, mutable clock, seeded id generator) and the single currently-running
 * {@link EngineInstance}.
 * <p>
 * The durable parts and the seams outlive a crash; only the {@link EngineInstance} (volatile engine state) is dropped
 * and rebuilt. The clock and scheduler are advanced in lock-step by {@link #advanceTime(Duration)} so the engine's
 * wall-clock timeout math ({@code ExecuteDelegate}'s per-attempt {@code orTimeout}) and its durable-delay scheduling
 * (wait timeouts, retry backoff) move together — that lock-step is what makes the {@code orTimeout} residual
 * deterministic (see README "Determinism level").
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class SimulationWorld implements AutoCloseable {

    private ControllableEventStorageEngine eventStore = new ControllableEventStorageEngine();
    private final DurableTokenStore tokenStore = new DurableTokenStore();
    private final MutableWorkflowHistoryRepository historyRepository = new InMemoryWorkflowHistoryRepository();
    private final CountingEffects effects = new CountingEffects();

    private final Instant epoch;
    private final MutableClock clock;
    private final SeededWorkflowIdGenerator idGenerator;
    // Non-final so crashAndRecoverWith(...) can rebuild the recovered engine under a DIFFERENT registration set (the
    // INV-18 drift-induction path: record under a v1 body, recover under a structurally-divergent v2 body). The default
    // crashAndRecover() leaves this untouched.
    private List<EngineInstance.WorkflowRegistration> registrations;

    private ManualWorkflowScheduler scheduler;
    private EngineInstance engine;
    // Optional extra component registrations applied to every engine this world builds (initial and recovered), the
    // seam a scenario uses to add a plain Axon event handler next to the workflow engine. Null = nothing extra.
    private final java.util.function.@org.jspecify.annotations.Nullable Consumer<ComponentRegistry> extraRegistrations;

    // Optional body-executor override (null = the engine's default virtual-thread executor, every existing path). Only
    // the INV-23 (EngineSelfProtection) nested-primitive deadlock probe sets this to a SameThreadExecutorService to
    // characterize the single-threaded-consumer "blocks forever" half of that self-protection surface.
        private final java.util.concurrent.@org.jspecify.annotations.Nullable ExecutorService bodyExecutorOverride;

    // OPT-IN aligned-clock mode (default false — see the aligned constructor's Javadoc for the trap this avoids).
    // When true, the JVM-global static Axon event-timestamp clock (ClockUtils) is pointed at THIS
    // world's MutableClock for the world's lifetime, so committed event timestamps and the injected engine clock
    // share ONE (virtual) era and the engine's recorded-STARTED-vs-now timer math is exact. The previous static value
    // is captured here and restored in close().
    private final boolean alignedEventClock;
    @org.jspecify.annotations.Nullable
    private final Clock previousEventClock;

    /**
     * Builds a fresh world anchored at the Unix epoch with the seam clocks seeded from {@code idSeed}, driving the
     * default {@link io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow} <strong>plus</strong> the two coexisting
     * versions of {@link io.axoniq.framework.workflow.simulation.workflow.VersionedOrderWorkflow} — so the fuzz/smoke run drives
     * version routing (INV-11) through the same crash/restart/reorder faults as the order workhorse. The versioned
     * definitions share one engine + registry with the order workflow; their distinct start event and id prefix
     * ({@code vorder-}) keep the two workflows independent.
     *
     * @param idSeed seed for the deterministic id generator's counter.
     */
    public SimulationWorld(long idSeed) {
        this(idSeed, (List<EngineInstance.WorkflowRegistration>) null);
    }

    /**
     * Builds a fresh world driving the single given workflow {@code registration} with an OPTIONAL body-executor
     * override. {@code null} keeps the engine's default virtual-thread executor (every other path). The INV-23
     * ({@code EngineSelfProtection}) nested-primitive deadlock probe passes a {@code SameThreadExecutorService} to
     * characterize the single-threaded-consumer "blocks forever" half of that self-protection surface; the harness's
     * SHORT wall-clock window then observes the deadlocked instance as non-terminal and the test finishes fast.
     *
     * @param idSeed               seed for the deterministic id generator's counter.
     * @param registration         the single workflow to register.
     * @param bodyExecutorOverride optional body {@code ExecutorService}; {@code null} keeps the default virtual-thread
     *                             executor.
     */
    public SimulationWorld(long idSeed, EngineInstance.WorkflowRegistration registration,
                           java.util.concurrent.@org.jspecify.annotations.Nullable ExecutorService bodyExecutorOverride) {
        this(idSeed, List.of(registration), bodyExecutorOverride, false);
    }

    /**
     * Builds a fresh world driving the single given workflow {@code registration} with the OPT-IN
     * <strong>aligned-clock</strong> mode. When {@code alignedEventClock} is {@code true}, the JVM-global static Axon
     * event-timestamp clock ({@code ClockUtils} — the source of every committed event's
     * {@code timestamp()}) is pointed at this world's {@link MutableClock} for the world's lifetime (restored to the
     * previous static value in {@link #close()}), so event timestamps and the engine's injected clock share ONE
     * (virtual) era. The engine's timer math compares the recorded STARTED timestamp against the injected clock
     * ({@code Duration.between(clock.instant(), startTime.plus(timeout))} in {@code WaitForDelegate}), so aligned mode
     * makes that arithmetic exact: a 10s wait timeout genuinely fires after a 10s virtual advance — no era anchoring —
     * and MULTI-step timeout sequences (a retry loop with several wait timeouts) become deterministically drivable.
     * <p>
     * <strong>Opt-in by design — do NOT make this the default.</strong> Every default-mode scenario and the whole
     * fuzz/smoke workload DEPEND on the documented D5 era gap (event timestamps in the wall era, the injected clock at
     * the Unix epoch, ~56 years apart): the engine's default 5s per-attempt step timeouts "never fire" because the
     * remaining time computes to ~+56y, which is exactly what keeps the workload green under the CLOCK_SKEW/CLOCK_JUMP
     * faults. Aligning globally would arm every default-timeout step against the virtual clock and doom the existing
     * workload. Default behaviour is byte-for-byte unchanged ({@code alignedEventClock=false} never touches the
     * static).
     * <p>
     * <strong>JVM-global static:</strong> {@code ClockUtils} holds a JVM-global static clock (Axon Framework
     * 5.1.1, explicitly documented "to fix the time while testing"), so an aligned-mode world must not run
     * concurrently with any other world (aligned or not) in the same JVM. The simulation tests are single-threaded
     * per JVM, so this holds; the value is captured at construction and restored in {@link #close()}.
     *
     * @param idSeed            seed for the deterministic id generator's counter.
     * @param registration      the single workflow to register.
     * @param alignedEventClock {@code true} to align the static event-timestamp clock with this world's virtual
     *                          clock for the world's lifetime; {@code false} keeps today's era-gap behaviour.
     */
    public SimulationWorld(long idSeed, EngineInstance.WorkflowRegistration registration,
                           boolean alignedEventClock) {
        this(idSeed, List.of(registration), null, alignedEventClock);
    }

    /**
     * Builds a fresh world driving the single given workflow {@code registration}. The crash/recover machinery is
     * registration-agnostic, so the dedicated scenarios (INV-7 {@code TerminalIsFinal}, INV-9 {@code TimeoutsFire},
     * INV-10 {@code OneInstancePerStart}, INV-11 {@code VersionRoutingSound}) reuse it to drive a single workflow across
     * the live-switch/replay boundary in isolation.
     *
     * @param idSeed       seed for the deterministic id generator's counter.
     * @param registration the single workflow to register.
     */
    public SimulationWorld(long idSeed, EngineInstance.WorkflowRegistration registration) {
        this(idSeed, List.of(registration));
    }

    /**
     * Builds a fresh world registering all the given workflow definitions, keeping the engine's default body executor.
     *
     * @param idSeed        seed for the deterministic id generator's counter.
     * @param registrations the workflow definitions to register, or {@code null} for the default multi-workflow set.
     */
    public SimulationWorld(long idSeed,
                           @org.jspecify.annotations.Nullable List<EngineInstance.WorkflowRegistration> registrations) {
        this(idSeed, registrations, null);
    }

    /**
     * Builds a fresh world registering <strong>all</strong> the given workflow definitions into one engine. Used both
     * for the default multi-workflow fuzz world (order + the two {@code VersionedOrderWorkflow} versions) and for the
     * INV-11 scenario (the two versioned definitions alone), so the multi-version registration path is exercised
     * identically in both.
     *
     * @param idSeed        seed for the deterministic id generator's counter.
     * @param registrations the workflow definitions to register (at least one), or {@code null} for the default
     *                      multi-workflow set (order workflow + the two versioned definitions).
     */
    public SimulationWorld(long idSeed,
                           @org.jspecify.annotations.Nullable List<EngineInstance.WorkflowRegistration> registrations,
                           java.util.concurrent.@org.jspecify.annotations.Nullable ExecutorService bodyExecutorOverride) {
        this(idSeed, registrations, bodyExecutorOverride, false);
    }

    /**
     * The full constructor: builds a fresh world registering <strong>all</strong> the given workflow definitions into
     * one engine, with the optional body-executor override and the OPT-IN aligned-clock mode (see
     * {@link #SimulationWorld(long, EngineInstance.WorkflowRegistration, boolean)} for the aligned-mode contract and
     * the trap that keeps it opt-in). Every other constructor delegates here with {@code alignedEventClock=false}, so
     * default behaviour is byte-for-byte unchanged.
     *
     * @param idSeed               seed for the deterministic id generator's counter.
     * @param registrations        the workflow definitions to register (at least one), or {@code null} for the default
     *                             multi-workflow set.
     * @param bodyExecutorOverride optional body {@code ExecutorService}; {@code null} keeps the default virtual-thread
     *                             executor.
     * @param alignedEventClock    {@code true} to align the JVM-global static event-timestamp clock
     *                             ({@code ClockUtils}) with this world's virtual clock for the world's
     *                             lifetime (restored in {@link #close()}); {@code false} keeps today's era-gap
     *                             behaviour.
     */
    public SimulationWorld(long idSeed,
                           @org.jspecify.annotations.Nullable List<EngineInstance.WorkflowRegistration> registrations,
                           java.util.concurrent.@org.jspecify.annotations.Nullable ExecutorService bodyExecutorOverride,
                           boolean alignedEventClock) {
        this(idSeed, registrations, bodyExecutorOverride, alignedEventClock, null);
    }

    /**
     * Creates a world whose engines additionally apply {@code extraRegistrations} to their component registry — the
     * seam for registering a plain Axon Framework event handler (an {@code EventProcessorModule}) next to the workflow
     * engine, applied again to every recovered engine so the consumer survives a crash like the engine does.
     *
     * @param idSeed             seed for the deterministic workflow id generator.
     * @param registrations      the workflow definitions to register (at least one).
     * @param extraRegistrations additional component registrations.
     * @return the world.
     */
    public static SimulationWorld withExtraRegistrations(
            long idSeed,
            List<EngineInstance.WorkflowRegistration> registrations,
            java.util.function.Consumer<ComponentRegistry> extraRegistrations) {
        return new SimulationWorld(idSeed, registrations, null, false, extraRegistrations);
    }

    private SimulationWorld(long idSeed,
                            @org.jspecify.annotations.Nullable List<EngineInstance.WorkflowRegistration> registrations,
                            java.util.concurrent.@org.jspecify.annotations.Nullable ExecutorService bodyExecutorOverride,
                            boolean alignedEventClock,
                            java.util.function.@org.jspecify.annotations.Nullable Consumer<ComponentRegistry> extraRegistrations) {
        this.extraRegistrations = extraRegistrations;
        this.epoch = Instant.EPOCH;
        this.clock = new MutableClock(epoch);
        this.bodyExecutorOverride = bodyExecutorOverride;
        this.alignedEventClock = alignedEventClock;
        // Align BEFORE anything (warmup events, engine startup) can stamp a timestamp, so the whole committed log of
        // an aligned world lives in the single virtual era. The previous static value (Axon's Clock.systemUTC()
        // default, unless a test changed it) is captured for restoration in close().
        if (alignedEventClock) {
            this.previousEventClock = ClockUtils.get();
            ClockUtils.set(this.clock);
        } else {
            this.previousEventClock = null;
        }
        this.idGenerator = new SeededWorkflowIdGenerator("uow", idSeed);
        // Compute the default set here (not via constructor delegation) so the registrations bind to THIS world's
        // freshly-created `effects` instance.
        this.registrations = registrations != null ? List.copyOf(registrations) : defaultRegistrations();
        this.scheduler = newScheduler();
        // Seed warmup events at indices 0,1 so workflow start triggers land at a positive index — see
        // ControllableEventStorageEngine#appendWarmup. This makes a reset-to-first-token replay include the triggers.
        eventStore.appendWarmup(new io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.WarmupEvent("warmup-1"));
        eventStore.appendWarmup(new io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.WarmupEvent("warmup-2"));
        this.engine = newEngine();
    }

    private ManualWorkflowScheduler newScheduler() {
        // Anchor the scheduler's virtual clock to the (possibly already-advanced) shared clock so a scheduler created
        // after a crash agrees with current virtual time.
        return new ManualWorkflowScheduler(clock.instant().toEpochMilli());
    }

    /**
     * The default multi-workflow set the fuzz/smoke world registers: the {@link OrderWorkflow} workhorse, both versions
     * of {@link io.axoniq.framework.workflow.simulation.workflow.VersionedOrderWorkflow}, the single
     * {@link io.axoniq.framework.workflow.simulation.workflow.MigratingOrderWorkflow}, and the single
     * {@link io.axoniq.framework.workflow.simulation.workflow.PayloadOrderWorkflow}, all sharing one engine. The versioned
     * definitions ride the same crash/restart/reorder faults as the order workflow, exercising INV-11 routing under
     * adversarial reordering; the migrating workflow's in-body {@code ctx.migrateVersion} record rides them too,
     * exercising INV-12 ({@code MigrateVersionContract}) where a replay-stability bug in the migration record would
     * show; the payload workflow's per-step payload writes ride them too, exercising INV-13
     * ({@code NoLostPayloadWrites}) where a crash/replay that lost a committed payload write would show; and the
     * {@link io.axoniq.framework.workflow.simulation.workflow.CombinatorWorkflow}'s three-branch combinator decisions ride them
     * too, exercising INV-14 ({@code CombinatorConsistency}) where a crash/replay that resolved a different combinator
     * decision would show; and the {@link io.axoniq.framework.workflow.simulation.workflow.CorrelatedWaitWorkflow}'s
     * key-correlated {@code waitForEvent} rides them too, exercising INV-15 ({@code EventCorrelationExact}) where a
     * cross-wakeup (an associated event waking a non-matching waiter) or a duplicate-driven second wait completion would
     * show; and the three coexisting versions of
     * {@link io.axoniq.framework.workflow.simulation.workflow.VersioningEdgesWorkflow} ride them too, exercising INV-20
     * ({@code VersioningEdges}) — its body performs two forward {@code ctx.migrateVersion} bumps under distinct
     * {@code changeId}s and a rejected downgrade — where a recorded downgrade, a marker re-applied on replay, or a
     * fresh spawn not at the highest of three registered versions would show; and the
     * {@link io.axoniq.framework.workflow.simulation.workflow.CustomNamedWorkflow}, registered with a custom
     * {@code eventNameCustomizer}, rides them too, exercising INV-22 ({@code EventNameCustomizationSound}) — where a
     * customized event name not applied, or a replay producing a different name under customization, would show.
     * <p>
     * The P-series production-realism workloads ride the same faults (all four settle cleanly under at-least-once
     * delivery + the fair horizon — the wedge/abandonment variants stay scenario-only): the
     * {@link io.axoniq.framework.workflow.simulation.workflow.SagaOrderWorkflow} compensation saga (retry-compensation variant,
     * happy mode — catch-branch compensation steps under the crash faults), the
     * {@link io.axoniq.framework.workflow.simulation.workflow.SubscriptionRenewalWorkflow} day-scale parked wait, the
     * {@link io.axoniq.framework.workflow.simulation.workflow.RollingDeployWorkflow} v1+v2 sibling registry (fresh spawn at v2
     * through the {@code migrateVersion} gate), and the
     * {@link io.axoniq.framework.workflow.simulation.workflow.LoopingPollWorkflow} counter-names retry loop.
     * <p>
     * The {@link io.axoniq.framework.workflow.simulation.workflow.PublishChainWorkflow} request/reply chain (three
     * definitions, three instances) rides them too, exercising INV-29 ({@code NoForeignStepRecorded}) and INV-30
     * ({@code PublisherObservesOwnPublish}): a published event is one durable record under its own type, it is the
     * publisher's completed step and nobody else's, it starts the responder and the observer exactly once, and it wakes
     * the requester's pre-registered wait.
     */
    private List<EngineInstance.WorkflowRegistration> defaultRegistrations() {
        var all = new ArrayList<EngineInstance.WorkflowRegistration>();
        all.add(EngineInstance.orderWorkflow(effects));
        all.addAll(EngineInstance.versionedOrderWorkflow(effects));
        all.add(EngineInstance.migratingOrderWorkflow(effects));
        all.add(EngineInstance.payloadOrderWorkflow(effects));
        all.add(EngineInstance.combinatorWorkflow(effects));
        all.add(EngineInstance.correlatedWaitWorkflow(effects));
        all.add(EngineInstance.reducerWorkflow(effects));
        all.addAll(EngineInstance.versioningEdgesWorkflow(effects));
        all.add(EngineInstance.customNamedWorkflow(effects));
        all.add(EngineInstance.sagaRetryCompOrderWorkflow(effects));
        all.add(EngineInstance.subscriptionRenewalWorkflow(effects));
        all.addAll(EngineInstance.rollingDeployWorkflowV1V2(effects));
        all.add(EngineInstance.loopingPollCounterNamesWorkflow(effects));
        all.addAll(EngineInstance.publishChainWorkflow(effects));
        return all;
    }

    private EngineInstance newEngine() {
        return new EngineInstance(eventStore, tokenStore, historyRepository, scheduler, clock, idGenerator,
                                  registrations, bodyExecutorOverride, extraRegistrations);
    }

    /**
     * Returns the currently running engine instance.
     *
     * @return the live engine.
     */
        public EngineInstance engine() {
        return engine;
    }

    /**
     * Returns the durable event store (the committed log + write-then-vanish control).
     *
     * @return the controllable event store.
     */
        public ControllableEventStorageEngine eventStore() {
        return eventStore;
    }

    /**
     * Returns the virtual-time scheduler.
     *
     * @return the scheduler.
     */
        public ManualWorkflowScheduler scheduler() {
        return scheduler;
    }

    /**
     * Returns the durable processor token store (diagnostics / advanced scenarios).
     *
     * @return the token store.
     */
        public DurableTokenStore tokenStore() {
        return tokenStore;
    }

    /**
     * Returns the mutable clock.
     *
     * @return the clock.
     */
        public MutableClock clock() {
        return clock;
    }

    /**
     * Returns the counting side-effect registry (survives crashes).
     *
     * @return the effect counters.
     */
        public CountingEffects effects() {
        return effects;
    }

    /**
     * Returns the committed workflow event log snapshot.
     *
     * @return the committed log, oldest first.
     */
        public List<EventMessage> committedLog() {
        return eventStore.committedWorkflowLog();
    }

    /**
     * Advances both the virtual-time scheduler and the clock by the same delta, in lock-step, so durable-delay firing
     * and the engine's wall-clock timeout math stay consistent.
     *
     * @param delta amount of virtual time to advance.
     */
    public void advanceTime(Duration delta) {
        clock.advanceBy(delta);
        scheduler.advanceBy(delta);
    }

    /**
     * Crashes the current engine (drops volatile state) and starts a fresh one over the same durable substrate —
     * a full crash + recovery cycle driving the real engine's replay path.
     * <p>
     * The safe-point store is frozen across the stop so the engine's <em>graceful</em> shutdown (which the harness
     * cannot avoid, since {@code WorkflowEngine::shutdown} is wired to the configuration shutdown lifecycle) cannot
     * advance the safe point past the work in flight. A real crash loses the process before it can persist a fresh
     * safe point, so recovery must reset to the safe point that was durable at crash time — which is exactly what
     * freezing preserves. The durable event log and effect counters are untouched.
     */
    public void crashAndRecover() {
        crashAndRecoverInternal();
    }

    /**
     * Crashes the current engine and recovers it under a <strong>different</strong> registration set, over the same
     * durable substrate. This is the INV-18 ({@code DriftGuardPausesCleanly}) drift-induction path: record an instance
     * under one (v1) body, then build the recovered engine with a structurally-divergent (v2) body that — sharing the
     * same {@code workflowName}/start event/id prefix/version — re-routes the SAME recorded instance to the divergent
     * code, WITHOUT a {@code ctx.migrateVersion}, tripping {@code guardAgainstReplayDrift} on the recovered replay.
     * <p>
     * All crash-modelling (frozen + pinned safe point, fresh event-store carrying the durable log, fresh scheduler) is
     * identical to {@link #crashAndRecover()}; the only difference is the recovered engine is built from
     * {@code recoveredRegistrations} instead of the world's original set. A real redeploy that ships divergent code
     * recovers exactly this way — the durable log is unchanged, only the running code differs. Keeping the existing
     * {@link #crashAndRecover()} behaviour byte-for-byte unchanged, this is additive and scenario-only.
     *
     * @param recoveredRegistrations the workflow definitions the recovered engine registers (at least one); they must
     *                               share {@code workflowName}/start event/id prefix/version with the original
     *                               registration to re-route the same recorded instance.
     */
    public void crashAndRecoverWith(List<EngineInstance.WorkflowRegistration> recoveredRegistrations) {
        if (recoveredRegistrations.isEmpty()) {
            throw new IllegalArgumentException("At least one recovered registration is required");
        }
        this.registrations = List.copyOf(recoveredRegistrations);
        crashAndRecoverInternal();
    }

    private void crashAndRecoverInternal() {
        // Model a real crash precisely: the dying process's token writes must NEVER leak past the crash boundary.
        //
        // A graceful stop (which the harness cannot avoid — WorkflowEngine shutdown is wired to the configuration
        // lifecycle) lets the dying processor checkpoint past its in-flight work. If such a write lands, the recovered
        // engine resets to that later token, decides no replay is needed, never re-creates the in-flight instance, and
        // the in-flight step is never resumed. A real crash loses the process before any such write reaches durable
        // storage, so the harness reproduces that: keep the store frozen across the whole crash+recovery and pin the
        // token that was durable at crash time.
        TrackingToken crashTimeToken = tokenStore.currentToken();
        tokenStore.freeze();
        engine.stop();
        // Re-pin the crash-time value: any leaked write from the dying engine (sync or async) was ignored while frozen,
        // but forcing it back guarantees the recovered engine reads exactly what was durable at crash time even if a
        // straggler callback raced in.
        if (crashTimeToken != null) {
            tokenStore.forceToken(crashTimeToken);
        }
        // Carry the durable committed log into a brand-new event-store instance for the recovered engine. A fresh
        // instance (vs. reusing the crashed engine's) has no leftover stream/coordinator state, so the recovered
        // processor cleanly replays the log (see ControllableEventStorageEngine#recoveredFrom). A vanished commit is
        // absent from the carried log, preserving the write-then-vanish (F-0) window.
        this.eventStore = ControllableEventStorageEngine.recoveredFrom(eventStore.committedTaggedEvents());
        // Durable-delay timers are volatile: a crashed process loses its scheduled sleep/backoff/wait-timeout
        // continuations. A fresh scheduler drops them so the crashed engine's stale (possibly never-due) timers do
        // not linger; the recovered engine re-schedules its own as it replays/resumes.
        this.scheduler = newScheduler();
        // Build the recovered engine while the store is STILL frozen. Its replay hook reads the pinned crash-time
        // token (fetch is never gated by the freeze), decides replay-vs-live correctly, replays, and switches to live
        // — all synchronously inside start(). Any safe-point writes it makes during that startup re-persist the same
        // crash-time token, so freezing them is harmless. Only once recovery is live do we unfreeze, so subsequent
        // real forward progress (an instance terminating) can advance the safe point normally.
        this.engine = newEngine();
        tokenStore.unfreeze();
    }

    @Override
    public void close() {
        try {
            engine.stop();
        } finally {
            // Aligned mode mutated the JVM-global static event-timestamp clock; restore the captured previous value
            // so a later (non-aligned) world in the same JVM sees the default wall-era behaviour again. Restored
            // AFTER engine.stop() so any events the dying engine still publishes stamp in the aligned era.
            if (alignedEventClock && previousEventClock != null) {
                ClockUtils.set(previousEventClock);
            }
        }
    }
}
