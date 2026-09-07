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
import io.axoniq.framework.workflow.runtime.api.execution.context.EventConditions;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Workflow that characterizes the <strong>blocking-convenience timeout-surfacing asymmetry</strong> (candidate finding
 * <strong>F-8</strong>): when a step TIMES OUT on a blocking convenience call, the typed {@code awaitEvent(stepName,
 * Class, ...)} overload special-cases {@code result.timeout()} and throws a clean {@code StepTimedOutException}, but the
 * {@code awaitExecute(...)} family and the <strong>untyped</strong> {@code awaitEvent(stepName, EventCondition)} go
 * through {@code AbstractDSLWorkflowContext.resolveStepPayload} and surface {@code StepFailedException} wrapping a
 * {@code null} cause instead.
 * <p>
 * The verified engine asymmetry this probes (read in source — see {@code formal/INVARIANTS.md} INV-9 and
 * {@code formal/POC-TLA-DST.adoc} Findings &rarr; F-8):
 * <ul>
 *   <li>{@code StateBasedWorkflowStepResult.error()} is <strong>always present</strong> (the step exists): it maps
 *       {@code step.error()} to a {@code StepFailedException}, but for a TIMED_OUT step
 *       {@code WorkflowStep.timedOut(name, payload, ts, ctx)} sets the error field to {@code null}, so {@code error()}
 *       returns {@code new StepFailedException(null)} (cause {@code null}).</li>
 *   <li>{@code AbstractDSLWorkflowContext.resolveStepPayload(result)}: {@code if (result.success()) {...} throw
 *       result.error().orElseThrow();} — for a TIMED_OUT step {@code success()} is false, so it throws the
 *       {@code StepFailedException(null)}.</li>
 *   <li>The timeout-surfacing fix was applied ONLY to the typed {@code awaitEvent(stepName, Class, ...)} overload
 *       ({@code SimpleWorkflowContext}), which checks {@code result.timeout()} and throws {@code StepTimedOutException};
 *       the {@code awaitExecute} family (typed {@code awaitExecute(String, Class, Supplier)} delegates to the untyped
 *       {@code awaitExecute(String, Map, processor, customizer)} then casts) and the untyped
 *       {@code awaitEvent(stepName, EventCondition)} both route through {@code resolveStepPayload}.</li>
 * </ul>
 * Three entry points, each driven on its own fresh world so a single blocking-convenience timeout is driven in
 * isolation (mirroring {@link RetryTimingWorkflow}'s two-half shape). Each catches the {@link Throwable} the blocking
 * convenience call surfaces and records the OBSERVED surface ({@link CapturedSurface}: the thrown class + whether its
 * cause is {@code null}) into a crash-surviving {@link AtomicReference}, then drives the workflow terminal with
 * {@code ctx.fail(t)} (the same catch + {@code ctx.fail} shape {@code examples/simple}'s {@code AwaitEventTimeoutWorkflow}
 * and the INV-9 {@link TimeoutWorkflow} use) so the harness observes a terminal instance:
 * <ol>
 *   <li>{@link #executeTimeoutAwaitExecute(SimpleWorkflowContext)} ({@code awaitexec-} ids) — the
 *       <strong>typed</strong> {@code awaitExecute(stepName, Class, Supplier)} on a {@code Supplier} that blocks on a
 *       never-released latch, so its (default 5s) per-attempt {@code execute} timeout elapses;</li>
 *   <li>{@link #executeTimeoutUntypedAwaitExecute(SimpleWorkflowContext)} ({@code awaitexecu-} ids) — the
 *       <strong>untyped</strong> {@code awaitExecute(stepName, Map, processor, customizer)} with an explicit short
 *       {@code timeout} on a blocking action;</li>
 *   <li>{@link #executeTimeoutUntypedAwaitEvent(SimpleWorkflowContext)} ({@code awaitevtu-} ids) — the
 *       <strong>untyped</strong> {@code awaitEvent(stepName, EventCondition)} on a {@link EventConditions#never()}
 *       condition (no event ever matches) with a short {@code timeout};</li>
 *   <li>{@link #executeTimeoutTypedAwaitEvent(SimpleWorkflowContext)} ({@code awaitevt-} ids) — the <strong>typed</strong>
 *       {@code awaitEvent(stepName, Class, conditions, customizer)} whose typed event is never delivered (the CONTRAST
 *       case that surfaces {@code StepTimedOutException} correctly, locking the asymmetry).</li>
 * </ol>
 * Each per-attempt {@code execute} timeout rides the non-injectable {@code orTimeout} residual (ARCHITECTURE.md §12 /
 * the adoc's D5), exactly like INV-9's wait timeout, so the scenario pre-advances the lock-step {@code MutableClock} past
 * the step's window; the untyped/typed {@code awaitEvent} wait timeouts ride the injectable scheduler. Scenario-pinned
 * only (not folded into the always-on fuzz): a deliberately-timing-out blocking convenience call surfaces an exception
 * the body catches, and the INV-9-style timeout machinery is kept out of the per-step fuzz set (Phase-3 D5).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class BlockingAwaitTimeoutWorkflow {

    /**
     * Logical workflow name for the typed-{@code awaitExecute} entry point, stable and distinct from the other
     * simulation workflows.
     */
    public static final String AWAIT_EXECUTE_WORKFLOW_NAME = "BlockingAwaitExecuteTimeoutWorkflow";

    /**
     * Logical workflow name for the untyped-{@code awaitExecute} entry point.
     */
    public static final String UNTYPED_AWAIT_EXECUTE_WORKFLOW_NAME = "BlockingUntypedAwaitExecuteTimeoutWorkflow";

    /**
     * Logical workflow name for the untyped-{@code awaitEvent} entry point.
     */
    public static final String UNTYPED_AWAIT_EVENT_WORKFLOW_NAME = "BlockingUntypedAwaitEventTimeoutWorkflow";

    /**
     * Logical workflow name for the typed-{@code awaitEvent} (contrast) entry point.
     */
    public static final String TYPED_AWAIT_EVENT_WORKFLOW_NAME = "BlockingTypedAwaitEventTimeoutWorkflow";

    /**
     * The blocking step name shared by all four entry points (each instance carries exactly one, on its own world).
     */
    public static final String STEP_BLOCKING = "blockingAwait";

    /**
     * The synthetic result type the typed {@code awaitExecute(stepName, Class, Supplier)} call asks for. Never produced
     * (the supplier blocks forever), so the call can only resolve via the timeout.
     */
    public static final Class<String> RESULT_TYPE = String.class;

    private final CountingEffects effects;
    private final CountDownLatch blockingLatch;
    private final AtomicReference<CapturedSurface> captured = new AtomicReference<>();

    /**
     * The observed exception surface a blocking convenience call threw on a timed-out step.
     *
     * @param thrownClassName  the fully-qualified class name of the {@link Throwable} the convenience call threw.
     * @param causeWasNull     whether that throwable's {@code getCause()} was {@code null} (the {@code F-8} signature for
     *                         the mis-classified {@code StepFailedException(null)} paths).
     * @param causeClassName   the fully-qualified class name of the cause, or {@code "<null>"} when the cause was null.
     */
    public record CapturedSurface(String thrownClassName, boolean causeWasNull,
                                  String causeClassName) {

    }

    /**
     * Creates the workflow bound to the given effect registry and a never-counted-down latch the {@code execute}-path
     * actions block on (so the per-attempt {@code orTimeout} window deterministically elapses).
     *
     * @param effects       registry that survives crashes; the pre-step bumps a counter here.
     * @param blockingLatch a latch the {@code execute}-path actions block on indefinitely (never counted down), so the
     *                      per-attempt {@code execute} timeout window elapses and the step records {@code TIMED_OUT}.
     */
    public BlockingAwaitTimeoutWorkflow(CountingEffects effects, CountDownLatch blockingLatch) {
        this.effects = effects;
        this.blockingLatch = blockingLatch;
    }

    /**
     * Exposes the exception surface captured around the blocking convenience call, so the scenario/test can assert on
     * the actual thrown type and whether its cause was {@code null}.
     *
     * @return the captured surface, or {@code null} if the convenience call returned normally (it should not — the work
     *         never completes).
     */
    @org.jspecify.annotations.Nullable
    public CapturedSurface capturedSurface() {
        return captured.get();
    }

    /**
     * Typed {@code awaitExecute(stepName, Class, Supplier)}: the supplier blocks on the never-released latch, so the
     * step's (default 5s) per-attempt {@code execute} timeout elapses and the step records {@code TIMED_OUT}. The typed
     * overload delegates to the untyped {@code awaitExecute(String, Map, processor)} then casts, so it surfaces whatever
     * {@code resolveStepPayload} throws.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeTimeoutAwaitExecute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        try {
            ctx.awaitExecute(STEP_BLOCKING, RESULT_TYPE, () -> {
                effects.record(workflowId, STEP_BLOCKING);
                blockForever();
                return "never-produced";
            });
        } catch (Throwable t) {
            capture(t);
            ctx.fail(t);
        }
    }

    /**
     * Untyped {@code awaitExecute(stepName, Map, processor, customizer)}: the action blocks on the never-released latch
     * under a short explicit {@code timeout}, so the per-attempt {@code execute} timeout elapses and the step records
     * {@code TIMED_OUT}. Surfaces whatever {@code resolveStepPayload} throws.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeTimeoutUntypedAwaitExecute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        try {
            ctx.awaitExecute(
                    STEP_BLOCKING,
                    Map.of(),
                    (pc, payload) -> {
                        effects.record(workflowId, STEP_BLOCKING);
                        blockForever();
                        return Map.of("done", true);
                    });
        } catch (Throwable t) {
            capture(t);
            ctx.fail(t);
        }
    }

    /**
     * Untyped {@code awaitEvent(stepName, EventCondition)}: the {@link EventConditions#never()} condition never matches,
     * so the wait times out (driven via the injectable scheduler) and the step records {@code TIMED_OUT}. Surfaces
     * whatever {@code resolveStepPayload} throws.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeTimeoutUntypedAwaitEvent(SimpleWorkflowContext ctx) {
        try {
            ctx.awaitEvent(
                    STEP_BLOCKING,
                    EventConditions.never(),
                    step -> step);
        } catch (Throwable t) {
            capture(t);
            ctx.fail(t);
        }
    }

    /**
     * Typed {@code awaitEvent(stepName, Class, conditions, customizer)} — the CONTRAST case. The typed event is never
     * delivered, so the wait times out; the typed overload special-cases {@code result.timeout()} and throws a clean
     * {@code StepTimedOutException}. Captured to lock the asymmetry against the mis-classified paths above.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeTimeoutTypedAwaitEvent(SimpleWorkflowContext ctx) {
        Object orderId = ctx.workflowPayload().get("orderId");
        try {
            ctx.awaitEvent(
                    STEP_BLOCKING,
                    SimulationEvents.PaymentConfirmedEvent.class,
                    io.axoniq.framework.workflow.runtime.association.Associations.associate(
                            io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty("orderId"),
                            io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo(orderId)),
                    step -> step);
        } catch (Throwable t) {
            capture(t);
            ctx.fail(t);
        }
    }

    /**
     * Records the observed surface of the throwable the blocking convenience call threw (its class + whether its cause
     * was {@code null}).
     */
    private void capture(Throwable t) {
        Throwable cause = t.getCause();
        captured.set(new CapturedSurface(t.getClass().getName(), cause == null,
                                         cause == null ? "<null>" : cause.getClass().getName()));
    }

    /**
     * Blocks on the never-released latch (responds to thread interruption so the engine shutdown can still unwind it).
     */
    private void blockForever() {
        try {
            blockingLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(STEP_BLOCKING + " interrupted while blocked past its timeout", e);
        }
    }
}
