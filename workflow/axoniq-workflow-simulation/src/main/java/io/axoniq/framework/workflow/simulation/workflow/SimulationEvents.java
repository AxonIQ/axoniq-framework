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

/**
 * Domain events used by the simulation's test workflows.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class SimulationEvents {

    private SimulationEvents() {
    }

    /**
     * Start event for the two-step "charge then confirm" workflow.
     *
     * @param orderId business key correlating the workflow instance and its awaited confirmation.
     */
    public record OrderPlacedEvent(String orderId) {

    }

    /**
     * External confirmation that wakes the workflow's {@code waitForEvent} step.
     *
     * @param orderId business key, matched against the waiting instance via an association.
     */
    public record PaymentConfirmedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.CancellingWorkflow}: a workflow that runs one
     * step and then cancels itself, used to exercise INV-7 ({@code TerminalIsFinal}) on the {@code ctx.cancel} terminal
     * path.
     *
     * @param orderId business key correlating the instance (prefixed {@code cancel-} by the id provider).
     */
    public record CancelRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.RetryingWorkflow}: a workflow whose flaky step
     * always fails under a retry policy, used to exercise INV-8 ({@code RetryBound}) on the retry-exhaustion path.
     *
     * @param orderId business key correlating the instance (prefixed {@code retry-} by the id provider).
     */
    public record RetryRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.TimeoutWorkflow}: a workflow whose
     * {@code waitForEvent} step always times out (its awaited {@link PaymentConfirmedEvent} is never delivered), used to
     * exercise INV-9 ({@code TimeoutsFire}) on the wait-timeout path.
     *
     * @param orderId business key correlating the instance (prefixed {@code timeout-} by the id provider).
     */
    public record TimeoutRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.StartOnlyWorkflow}: a workflow that runs one
     * step and then suspends on a never-arriving {@code waitForEvent}, staying LIVE so a duplicate/redelivered start can
     * be deduped while the instance is live, used to exercise INV-10 ({@code OneInstancePerStart}).
     *
     * @param orderId business key correlating the instance (prefixed {@code start-} by the id provider).
     */
    public record StartOnlyRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.VersionedOrderWorkflow}: a workflow registered
     * at two coexisting versions (same {@code workflowName}/start event, different {@code workflowVersion}), used to
     * exercise INV-11 ({@code VersionRoutingSound}) — a fresh start spawns at the highest registered version and every
     * event for the instance carries exactly that one version, deterministically across crash/replay.
     *
     * @param orderId business key correlating the instance (prefixed {@code vorder-} by the id provider).
     */
    public record VersionedOrderRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.MigratingOrderWorkflow}: a single workflow whose
     * body calls {@code ctx.migrateVersion(changeId, newVersion)} to fork mid-flight, used to exercise INV-12
     * ({@code MigrateVersionContract}) — the recorded migration version for a {@code changeId} is written at most once,
     * is monotonic non-decreasing (first-writer-wins, never downgrades), and replaying the same history resolves the
     * same recorded version unchanged.
     *
     * @param orderId business key correlating the instance (prefixed {@code vmig-} by the id provider).
     */
    public record MigrateRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.PayloadOrderWorkflow}: a workflow whose every
     * step writes a distinct key to the workflow payload, used to exercise INV-13 ({@code NoLostPayloadWrites}) — the
     * final committed payload must reflect every committed step's recorded contribution, so no committed payload write
     * is lost across crashes/replays (modulo an intended overwrite by a later step on the same key).
     *
     * @param orderId business key correlating the instance (prefixed {@code payload-} by the id provider).
     */
    public record PayloadOrderRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.VersioningEdgesWorkflow}: a workflow registered
     * at <strong>three</strong> coexisting versions (same {@code workflowName}/start event, different
     * {@code workflowVersion}) whose body performs two in-body {@code ctx.migrateVersion} forward bumps under distinct
     * {@code changeId}s and then attempts a third migration that is a <strong>downgrade</strong>, used to exercise INV-20
     * ({@code VersioningEdges}) — the versioning edges INV-11/INV-12 do not cover: a downgrade migration is rejected
     * (never recorded), multiple distinct {@code changeId}s each record at most once and monotonic non-decreasing, and a
     * deeper (3-version) registry routes a fresh start to the highest version and an instance recorded at an older
     * version to the closest sibling (never 0, never 2 definitions).
     *
     * @param orderId business key correlating the instance (prefixed {@code vedge-} by the id provider).
     */
    public record VersioningEdgesRequestedEvent(String orderId) {

    }

    /**
     * The signal event the {@link io.axoniq.framework.workflow.simulation.workflow.VersioningEdgesWorkflow}'s body waits for
     * (correlated on {@code orderId}). The scenario suspends the instance on this wait while it is still recorded at the
     * highest version, crashes, recovers under a registry whose highest version was dropped, then delivers this signal
     * so the resumed body runs its final step under the closest-sibling definition the 4/5-pass lookup routed to — the
     * observable that the routing landed on a runnable body (never 0, never 2).
     *
     * @param orderId business key, matched against the waiting instance via an association.
     */
    public record VersioningEdgesSignalEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.CombinatorWorkflow}: a workflow that runs three
     * parallel {@code execute} branches and folds them through all three combinators ({@code anyMatch}/{@code allMatch}/
     * {@code noneMatch}), used to exercise INV-14 ({@code CombinatorConsistency}) — each combinator's decision must be
     * consistent with the documented short-circuit semantics, a pure function of its branch steps' committed terminal
     * outcomes, and unchanged after a crash + replay.
     *
     * @param orderId business key correlating the instance (prefixed {@code comb-} by the id provider).
     */
    public record CombinatorRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.ReducerWorkflow}: a workflow whose steps
     * deterministically exercise all three payload reducers — a {@code global_only} {@code execute} result (discarded), a
     * {@code combine_local_and_global} {@code execute} result (merged), a {@code local_only} {@code modifyPayload}
     * replace (whole-payload overwrite), plus a {@code parameterPayloadReducer} step demonstrating the parameter-side
     * input view — used to exercise INV-19 ({@code PayloadReducerSemantics}) — each reducer produces its documented merge
     * into the engine's reconstructed payload, stable across crash/replay.
     *
     * @param orderId business key correlating the instance (prefixed {@code reducer-} by the id provider).
     */
    public record ReducerRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.ReducerWorkflow}'s null-value edge body
     * ({@code ReducerWorkflow#nullEdge}): a deterministic instance whose {@code combine} step's result map carries a
     * {@code null} value under a key (edge (a) of INV-19 {@code PayloadReducerSemantics}). Scenario-pinned (not folded
     * into the always-on fuzz) so the engine's actual {@code null}-under-combine handling can be observed and
     * characterized without a serialization quirk flaking the 1000-seed sweep.
     *
     * @param orderId business key correlating the instance (prefixed {@code reducer-} by the id provider).
     */
    public record ReducerNullEdgeRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.ReducerWorkflow}'s throwing-modifier body
     * ({@code ReducerWorkflow#throwingModifier}): a deterministic instance whose {@code modifyPayload} modifier lambda
     * throws a plain {@code RuntimeException} between primitives (the S-4 generalization of edge (a)/finding F-6 — a
     * broader trigger family for the {@code handleWorkflowException} {@code default}-branch wedge). Scenario-pinned (not
     * folded into the always-on fuzz) so the engine's actual handling of an unexpected runtime exception from a
     * between-primitives user lambda can be observed and characterized: the instance is left non-terminal (a liveness
     * stall), the exception only logged, and replay re-hits it (recovery-unsafe).
     *
     * @param orderId business key correlating the instance (prefixed {@code reducer-} by the id provider).
     */
    public record ReducerThrowingModifierRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.CorrelatedWaitWorkflow}: a workflow that runs a
     * setup step then suspends on a {@code waitForEvent} correlated on its <em>own</em> per-instance key, used to
     * exercise INV-15 ({@code EventCorrelationExact}) — an {@code associate(...)}-correlated event wakes EXACTLY the
     * matching waiting instance (never a non-matching one — no cross-wakeup), and duplicate/uncorrelated events produce
     * no spurious wait completion.
     *
     * @param orderId business key correlating the instance (prefixed {@code corr-} by the id provider).
     * @param key     the per-instance correlation key the {@code waitForEvent} step associates on; the matching
     *                {@link CorrelatedSignalEvent} carries the same {@code key}.
     */
    public record CorrelatedWaitRequestedEvent(String orderId, String key) {

    }

    /**
     * Start event for the <strong>no-retry</strong> {@link io.axoniq.framework.workflow.simulation.workflow.FailingWorkflow}: a
     * workflow whose failing step throws an uncaught exception with no retry budget, so the failure propagates to a
     * terminal FAILED workflow status immediately, used to exercise INV-16 ({@code FailurePropagation}) on the
     * no-retry path.
     *
     * @param orderId business key correlating the instance (prefixed {@code fail-} by the id provider).
     */
    public record FailRequestedEvent(String orderId) {

    }

    /**
     * Start event for the <strong>retry-exhaustion</strong> {@link io.axoniq.framework.workflow.simulation.workflow.FailingWorkflow}:
     * a workflow whose failing step throws on every attempt and exhausts its retry policy, so the failure propagates to a
     * terminal FAILED workflow status after exhaustion, used to exercise INV-16 ({@code FailurePropagation}) on the
     * retry-exhaustion path (distinct from INV-8's attempt-count bound on the same shape).
     *
     * @param orderId business key correlating the instance (prefixed {@code failretry-} by the id provider).
     */
    public record FailExhaustionRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.HookWorkflow}: a workflow that runs one step and
     * then completes, with a counting status-change listener registered on STARTED and COMPLETED, used to exercise
     * INV-17 ({@code StatusHookFiresOncePerStatus}) — each registered status's hook fires exactly once for the instance
     * and is not re-fired on crash/replay.
     *
     * @param orderId business key correlating the instance (prefixed {@code hook-} by the id provider).
     */
    public record HookRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.DriftWorkflow}: a workflow recorded under one
     * (v1) body then replayed under a structurally-divergent (v2) body WITHOUT {@code ctx.migrateVersion}, used to
     * induce replay drift and exercise INV-18 ({@code DriftGuardPausesCleanly}) — when {@code guardAgainstReplayDrift}
     * throws {@code WorkflowReplayDriftException}, the engine pauses the instance non-terminally and cleanly (no
     * terminal workflow-status event, no spurious step event, committed history intact).
     *
     * @param orderId business key correlating the instance (prefixed {@code drift-} by the id provider).
     */
    public record DriftRequestedEvent(String orderId) {

    }

    /**
     * The never-delivered signal the {@link io.axoniq.framework.workflow.simulation.workflow.DriftWorkflow}'s v1 body waits for
     * (correlated on {@code orderId}). It is deliberately never published, so the v1-recorded instance stays LIVE /
     * non-terminal (with {@code chargePayment} durably COMPLETED) — the state a divergent v2 replay drifts against.
     *
     * @param orderId business key, matched against the waiting instance via an association.
     */
    public record DriftSignalEvent(String orderId) {

    }

    /**
     * The dedicated signal event that wakes a {@link io.axoniq.framework.workflow.simulation.workflow.CorrelatedWaitWorkflow}'s
     * {@code waitForEvent} step — but <strong>only</strong> the instance whose correlation key equals this event's
     * {@code key} (the {@code waitForEvent} associates on {@code payloadProperty("key") = ownKey}, exactly as
     * {@link OrderWorkflow}'s {@code awaitConfirmation} associates on {@code orderId}). Used to exercise INV-15
     * ({@code EventCorrelationExact}): delivering {@code CorrelatedSignalEvent(key=A)} must wake only instance A (never
     * instance B waiting on a distinct key — no cross-wakeup); an uncorrelated key (no waiter) wakes nobody; a duplicate
     * of an already-matched key produces no second wait completion.
     *
     * @param key the correlation key; matched against the waiting instance's own key via an association.
     */
    public record CorrelatedSignalEvent(String key) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.RetryTimingWorkflow}'s <strong>retry-edges</strong>
     * entry point: a workflow whose steps exercise the three {@code BackoffStrategy}s (fixed/linear/exponential), a
     * {@code RetryPolicy.retryWhile(predicate)} that stops before {@code maxRetries}, and a crash-surviving {@code onRetry}
     * fire counter, used to exercise INV-21 ({@code RetryTimingAndExhaustionEdges}) on the retry/backoff edges INV-8 did
     * not cover — backoff-strategy timing reconstructed from the recorded {@code RETRYING} timestamps (survives
     * crash/replay), the {@code retryWhile} predicate bounding the attempt records, and the {@code onRetry} handler firing
     * once per actual retry decision and NOT re-firing on crash/replay.
     *
     * @param orderId business key correlating the instance (prefixed {@code retryedge-} by the id provider).
     */
    public record RetryEdgesRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.RetryTimingWorkflow}'s
     * <strong>per-attempt-execute-timeout</strong> entry point: a workflow whose {@code execute} step blocks past its
     * per-attempt {@code timeout}, used to exercise INV-21 ({@code RetryTimingAndExhaustionEdges}) on the per-attempt
     * {@code execute}-timeout edge (the D5 residual INV-9 left to the wait path) — the step reaches a terminal
     * {@code TIMED_OUT} outcome and never hangs, with the total budget {@code (retries + 1) × timeout}.
     *
     * @param orderId business key correlating the instance (prefixed {@code retrytimeout-} by the id provider).
     */
    public record RetryTimeoutRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.CustomNamedWorkflow}: a workflow registered with
     * a custom {@code eventNameCustomizer} (custom namespace + workflow base name + a {@code stepCompleted} status-suffix
     * override), used to exercise INV-22 ({@code EventNameCustomizationSound}) — every committed step/status event of the
     * instance carries the EXPECTED customized wire name, those names are stable across crash/replay (a pure function of
     * history), and the engine still routes/replays correctly under customization.
     *
     * @param orderId business key correlating the instance (prefixed {@code named-} by the id provider).
     */
    public record CustomNamedRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.NestedPrimitiveWorkflow}: a workflow whose
     * {@code execute} action lambda calls ANOTHER primitive ({@code ctx.awaitExecute(...)} from inside the outer step's
     * action) — the §3.1-forbidden nested-primitive anti-pattern — used to PROBE the engine's self-protection at this
     * failure surface (INV-23 {@code EngineSelfProtection}): does the engine detect/reject the nested call (a clear
     * exception → an observable terminal/abort), or silently DEADLOCK the per-instance single-threaded task queue (the
     * inner {@code appendTask} can never be consumed because the consumer is busy executing the outer task that is
     * blocking on it)?
     *
     * @param orderId business key correlating the instance (prefixed {@code selfprot-} by the id provider).
     */
    public record NestedPrimitiveRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.DuplicatePayloadWorkflow}: a workflow whose body
     * runs a {@code modifyPayload} step that is NOT the last step (the next step is a never-arriving {@code waitForEvent}
     * that keeps the instance LIVE), used to settle the S-1 candidate and exercise INV-2 ({@code AtMostOnceRecording}) at
     * the {@code modifyPayload}-on-post-crash-live-re-run surface (finding <strong>F-7</strong>) — the
     * {@code modifyPayload} {@code <step>:COMPLETED} commits, the instance stays non-terminal on the wait, then a crash +
     * recover re-runs the body and {@code PayloadDelegate} re-publishes a SECOND {@code <step>:COMPLETED} (bypassing the
     * terminal-step guard {@code ExecuteDelegate}/{@code sendStepEvent} would have applied).
     *
     * @param orderId business key correlating the instance (prefixed {@code duppay-} by the id provider).
     */
    public record DuplicatePayloadRequestedEvent(String orderId) {

    }

    /**
     * The never-delivered signal the {@link io.axoniq.framework.workflow.simulation.workflow.DuplicatePayloadWorkflow}'s body waits
     * for (correlated on {@code orderId}). It is deliberately never published, so the instance stays LIVE / non-terminal
     * (with {@code finalizePayload} durably COMPLETED) — the state a crash + recover re-runs the body against, re-reaching
     * the {@code modifyPayload} call (the F-7 duplicate-publish window).
     *
     * @param orderId business key, matched against the waiting instance via an association.
     */
    public record DuplicatePayloadSignalEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow}'s
     * <strong>typed-{@code awaitExecute}</strong> entry point: a workflow whose typed {@code awaitExecute(stepName, Class,
     * Supplier)} action blocks past its per-attempt {@code execute} timeout, used to settle the S-2 candidate (finding
     * <strong>F-8</strong>) — the blocking-convenience timeout-surfacing asymmetry — and observe what exception type the
     * blocking convenience call surfaces on a timed-out step.
     *
     * @param orderId business key correlating the instance (prefixed {@code awaitexec-} by the id provider).
     */
    public record BlockingAwaitExecuteTimeoutRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow}'s
     * <strong>untyped-{@code awaitExecute}</strong> entry point: a workflow whose untyped
     * {@code awaitExecute(stepName, Map, processor, customizer)} action blocks past a short explicit {@code timeout}, used
     * to settle the S-2 candidate (finding <strong>F-8</strong>).
     *
     * @param orderId business key correlating the instance (prefixed {@code awaitexecu-} by the id provider).
     */
    public record BlockingUntypedAwaitExecuteTimeoutRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow}'s
     * <strong>untyped-{@code awaitEvent}</strong> entry point: a workflow whose untyped
     * {@code awaitEvent(stepName, EventCondition)} ({@code EventConditions.never()}) times out, used to settle the S-2
     * candidate (finding <strong>F-8</strong>).
     *
     * @param orderId business key correlating the instance (prefixed {@code awaitevtu-} by the id provider).
     */
    public record BlockingUntypedAwaitEventTimeoutRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow}'s
     * <strong>typed-{@code awaitEvent}</strong> CONTRAST entry point: a workflow whose typed
     * {@code awaitEvent(stepName, Class, conditions, customizer)} event is never delivered and times out — the path that
     * correctly surfaces {@code StepTimedOutException}, locking the asymmetry the S-2 candidate (finding
     * <strong>F-8</strong>) settles.
     *
     * @param orderId business key correlating the instance (prefixed {@code awaitevt-} by the id provider).
     */
    public record BlockingTypedAwaitEventTimeoutRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.AnyMatchNoMatchWorkflow}: a workflow that runs
     * three {@code execute} branches that ALL complete but NONE satisfies the combinator predicate, then reads the
     * {@code anyMatch} winner-derived accessors, used to settle the candidate finding S-5 and extend INV-14
     * ({@code CombinatorConsistency}) coverage — characterizing the {@code anyMatch} winner-result semantics on the
     * no-predicate-match-but-all-completed path (the engine sets {@code fallback.orElse(results[0])} as the winner, so the
     * winner-derived {@code success()}/{@code result()}/{@code resultAs()} read the first completed branch even though no
     * branch matched).
     *
     * @param orderId business key correlating the instance (prefixed {@code anynomatch-} by the id provider).
     */
    public record AnyMatchNoMatchRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.BackoffOverflowWorkflow}: a workflow whose single
     * always-failing {@code execute} step runs under a LARGE {@code maxRetries} with {@code BackoffStrategy.exponential},
     * used to PROBE the candidate finding S-3 (the {@code BackoffStrategy.exponential} shift/{@code Duration} overflow at
     * large attempt counts) and exercise INV-8 ({@code RetryBound}) / INV-21 ({@code RetryTimingAndExhaustionEdges}) at the
     * backoff-arithmetic edge — the engine retries until the exponential factor overflows {@code Duration} and the
     * resulting {@code ArithmeticException} (on the workflow thread) wedges the instance non-terminally (the
     * {@code handleWorkflowException} {@code default}-branch sink, the F-6/S-4 wedge).
     *
     * @param orderId business key correlating the instance (prefixed {@code boverflow-} by the id provider).
     */
    public record BackoffOverflowRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.SagaOrderWorkflow} <strong>no-compensation-retry</strong>
     * variant: a realistic production order-fulfillment saga (reserve stock, charge payment with retries, await an
     * external fulfillment confirmation under a timeout, notify on success) whose catch branches run
     * <strong>compensation steps</strong> (release stock, refund payment) before terminating via {@code ctx.cancel()}
     * (timeout) or {@code ctx.fail(...)} (charge failure). The compensation steps carry NO retry policy — the fragile
     * but common authoring the Phase-1 production-realism scenarios probe under crash windows.
     *
     * @param orderId business key correlating the instance (prefixed {@code saga-} by the id provider).
     * @param mode    behaviour selector carried in the start payload ({@code happy} or {@code chargeDeclined}); read
     *                deterministically from the workflow payload, never from external state.
     */
    public record SagaOrderPlacedEvent(String orderId, String mode) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.SagaOrderWorkflow} <strong>compensation-retry</strong>
     * variant: the same saga shape, but the compensation steps carry a retry policy
     * ({@code maxRetries(SagaOrderWorkflow.COMPENSATION_MAX_RETRIES)}) — the recommended production authoring whose
     * crash-resilience the Phase-1 scenarios contrast against the no-retry variant.
     *
     * @param orderId business key correlating the instance (prefixed {@code sagarc-} by the id provider).
     * @param mode    behaviour selector carried in the start payload ({@code happy} or {@code chargeDeclined}).
     */
    public record SagaRetryCompOrderPlacedEvent(String orderId, String mode) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.SagaOrderWorkflow}
     * <strong>default-per-attempt-timeout</strong> variant (the doomed-attempt probe): the same saga shape with the
     * engine's default 5s per-attempt timeout pinned on every {@code execute} step, so a step entered after the clock
     * moved past {@code STARTED + timeout} (forward clock jump in production; the era-crossing virtual advance in the
     * harness) exposes the engine's dispatch-before-deadline-check behaviour.
     *
     * @param orderId business key correlating the instance (prefixed {@code sagadt-} by the id provider).
     * @param mode    behaviour selector carried in the start payload ({@code happy} or {@code chargeDeclined}).
     */
    public record SagaDefaultTimeoutOrderPlacedEvent(String orderId, String mode) {

    }

    /**
     * External fulfillment confirmation the saga's {@code awaitFulfillment} wait step correlates on by {@code orderId}.
     * Delivering it before the wait's timeout completes the happy path; withholding it past the timeout drives the
     * timeout-compensation branch.
     *
     * @param orderId business key matching the waiting saga instance's association.
     */
    public record FulfillmentConfirmedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.SubscriptionRenewalWorkflow}: the Phase-2
     * production-realism <strong>long-parked</strong> workload — registers a subscription then parks for up to 30 days
     * on an external renewal decision, surviving restarts, churn and clock skips around it.
     *
     * @param orderId business key correlating the instance (prefixed {@code subs-} by the id provider).
     */
    public record SubscriptionStartedEvent(String orderId) {

    }

    /**
     * The external renewal decision the parked {@code awaitRenewalDecision} wait step correlates on by
     * {@code orderId}. Delivering it wakes the parked instance to the renewal path; withholding it past the 30-day
     * window drives the expiry path.
     *
     * @param orderId business key matching the waiting instance's association.
     */
    public record RenewalDecidedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.RollingDeployWorkflow}: the Phase-3
     * production-realism <strong>rolling-deploy</strong> workload — an order that parks on an external approval while
     * the registry changes across recoveries (v2 added, v1 removed prematurely, a bad deploy rolled back).
     *
     * @param orderId business key correlating the instance (prefixed {@code deploy-} by the id provider).
     */
    public record RollingDeployOrderEvent(String orderId) {

    }

    /**
     * The external approval the parked {@code awaitApproval} wait step correlates on by {@code orderId}.
     *
     * @param orderId business key matching the waiting instance's association.
     */
    public record ApprovalGrantedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.LoopingPollWorkflow} <strong>reused-names</strong>
     * variant (the canonical-example shape: the loop reuses the SAME step names every iteration — the authoring the
     * Phase-4 live-lock probe characterizes).
     *
     * @param orderId business key correlating the instance (prefixed {@code loopr-} by the id provider).
     */
    public record LoopReusedPollRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.LoopingPollWorkflow}
     * <strong>counter-names</strong> variant (the documented correct authoring: per-iteration step names derived from
     * a deterministic loop counter).
     *
     * @param orderId business key correlating the instance (prefixed {@code loopc-} by the id provider).
     */
    public record LoopCounterPollRequestedEvent(String orderId) {

    }

    /**
     * The external signal the loop's {@code pollSignal} wait correlates on by {@code orderId}.
     *
     * @param orderId business key matching the polling instance's association.
     */
    public record PollSignalEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.CombinatorReplayWorkflow}: the combinator
     * replay-determinism probes (does {@code matched()}/{@code unmatched()}/the {@code anyMatch} winner recompute the
     * same way after a crash/replay?).
     *
     * @param orderId business key correlating the instance (prefixed {@code creplay-} by the id provider).
     * @param mode    probe selector ({@code allMatchLate} / {@code anyMatchWinner} / {@code noneMatchTimeout}).
     */
    public record CombinatorReplayRequestedEvent(String orderId, String mode) {

    }

    /**
     * Completes the combinator probe's branch-A wait, correlated by {@code orderId}.
     *
     * @param orderId business key matching the probing instance.
     */
    public record BranchASignalEvent(String orderId) {

    }

    /**
     * Completes the combinator probe's branch-B wait, correlated by {@code orderId}.
     *
     * @param orderId business key matching the probing instance.
     */
    public record BranchBSignalEvent(String orderId) {

    }

    /**
     * Completes the combinator probe's late branch-C wait, correlated by {@code orderId}.
     *
     * @param orderId business key matching the probing instance.
     */
    public record BranchCSignalEvent(String orderId) {

    }

    /**
     * Releases the combinator probe's post-snapshot gate wait, correlated by {@code orderId} — the park that keeps
     * the instance alive across the crash window.
     *
     * @param orderId business key matching the probing instance.
     */
    public record GateOpenedEvent(String orderId) {

    }

    /**
     * A no-op warmup event that no workflow starts on or waits for. The simulator appends a couple of these before any
     * workflow so that workflow start triggers land at a positive event-store index; recovery can then reset to the
     * first token and replay from there <em>inclusive</em> of those triggers (an event-store reset position is
     * exclusive of the position itself), exactly as {@code WorkflowReplayPreparedStateTest} does with its warmup
     * events.
     *
     * @param id an opaque identifier.
     */
    public record WarmupEvent(String id) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow}: a workflow
     * that suspends on a blocking {@code awaitEvent} (correlated on {@code orderId}, its {@link CorrelatedSignalEvent}
     * deliberately never published), catches the {@code StepCancellationException} an external
     * {@code cancelRunningStep(...)} surfaces, and compensates to normal completion. Used by the external
     * step-cancellation scenario to drive a cancel from a thread other than the workflow's own control thread.
     *
     * @param orderId business key; also the correlation key the blocking wait associates on.
     */
    public record ExternalCancelRequestedEvent(String orderId) {

    }

    /**
     * Start event for the {@link io.axoniq.framework.workflow.simulation.workflow.BackoffCancelWorkflow}: a workflow whose
     * retrying {@code execute} step sits in a long fixed-backoff window while the body parks on a correlated wait.
     * The {@code mode} field selects the drive: {@code "cancel"} (the body issues {@code ctx.cancel(...)} while the
     * step is in backoff) or {@code "external"} (the scenario cancels the retrying step via
     * {@code cancelRunningStep(...)} instead). Used by the backoff-cancellation scenario to show that a cancellation
     * landing in a backoff window is not honored (the backoff future has no cancellation-to-publish wiring).
     *
     * @param orderId business key; also the correlation key the wait associates on.
     * @param mode    {@code "cancel"} or {@code "external"}.
     */
    public record BackoffCancelRequestedEvent(String orderId, String mode) {

    }
}
