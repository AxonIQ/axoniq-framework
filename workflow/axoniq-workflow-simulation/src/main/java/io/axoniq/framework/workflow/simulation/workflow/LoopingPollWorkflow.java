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
import io.axoniq.framework.workflow.runtime.api.execution.state.StepFailedException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepInterruptedException;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PollSignalEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * The Phase-4 production-realism <strong>retry-loop</strong> workload: the documented loop-with-event-wait recipe
 * (poll for an external signal under a timeout; on timeout, sleep and retry) in two authorings:
 * <ul>
 *   <li>{@link #executeReusedNames reused names} — the loop reuses the SAME step names every iteration, exactly like
 *       the canonical {@code PaymentWorkflow} example ({@code "paymentPrepared"}/{@code "retryPayment"} with no
 *       counter). Step names are durable dedup keys, so iteration 2 onward the wait returns iteration 1's CACHED
 *       TIMED_OUT instantly (never re-registering — blind to fresh signals) and the loop degenerates to a hot spin;
 *       worse, {@code ctx.sleep} is NON-blocking (contrary to its convenience Javadoc), so re-entering the
 *       still-STARTED sleep queues DUPLICATE timedOut publishes gated only on lagging in-memory state — multiple
 *       terminal records for one step. Bounded here by {@link #MAX_SPINS} so the characterization is finite;</li>
 *   <li>{@link #executeCounterNames counter names} — per-iteration step names derived from the deterministic loop
 *       counter, the documented correct authoring; each iteration records its own wait/sleep and the loop genuinely
 *       retries.</li>
 * </ul>
 * Execute steps carry generous per-attempt timeouts ({@link SagaOrderWorkflow#GENEROUS_STEP_TIMEOUT} — the F-14/D5
 * era residual).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class LoopingPollWorkflow {

    /**
     * Logical workflow name of the reused-names variant.
     */
    public static final String WORKFLOW_NAME_REUSED = "LoopingPollReusedNamesWorkflow";

    /**
     * Logical workflow name of the counter-names variant.
     */
    public static final String WORKFLOW_NAME_COUNTER = "LoopingPollCounterNamesWorkflow";

    /**
     * The poll wait step (reused verbatim every iteration in the reused-names variant; suffixed {@code #i} in the
     * counter-names variant).
     */
    public static final String STEP_POLL = "pollSignal";

    /**
     * The retry-backoff sleep step (same naming convention as {@link #STEP_POLL}).
     */
    public static final String STEP_RETRY_DELAY = "retryDelay";

    /**
     * The post-loop processing step — counting side effect, reached only when a poll succeeded.
     */
    public static final String STEP_PROCESS = "processSignal";

    /**
     * Per-iteration effect counter key — counts BODY iterations (vs the durable records, whose divergence is the
     * live-lock observable).
     */
    public static final String EFFECT_ITERATION = "loopIteration";

    /**
     * The poll wait timeout per iteration.
     */
    public static final Duration POLL_TIMEOUT = Duration.ofSeconds(10);

    /**
     * The retry-backoff sleep per iteration.
     */
    public static final Duration RETRY_DELAY = Duration.ofSeconds(2);

    /**
     * Bound on loop iterations so the reused-names hot spin is a finite, observable characterization rather than a
     * genuine infinite live-lock in the test JVM. A production body would spin forever.
     */
    public static final int MAX_SPINS = 50;

    /**
     * How many poll attempts the counter-names probe lets time out before delivering the signal.
     */
    public static final int COUNTER_TIMEOUT_ITERATIONS = 2;

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; iteration and step counters land here.
     */
    public LoopingPollWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The canonical-example authoring: step names reused verbatim across iterations. Iteration 1 records the wait's
     * TIMED_OUT and the sleep's COMPLETED; iterations 2..{@link #MAX_SPINS} read those cached terminals instantly and
     * append nothing — the loop spins hot, blind to fresh signals (no wait condition is ever re-registered), until
     * the bound trips and the body fails explicitly.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeReusedNames(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");

        boolean delivered = false;
        int spins = 0;
        while (!delivered && spins < MAX_SPINS) {
            spins++;
            effects.record(workflowId, EFFECT_ITERATION);

            var signal = ctx.waitForEvent(
                    STEP_POLL,
                    SimulationEvents.PollSignalEvent.class,
                    associate(payloadProperty("orderId"), equalsTo(orderId)),
                    step -> step.timeout(POLL_TIMEOUT)
            );
            signal.await();
            if (signal.success()) {
                delivered = true;
            } else {
                ctx.sleep(STEP_RETRY_DELAY, RETRY_DELAY);
            }
        }

        if (delivered) {
            process(ctx, workflowId);
        } else {
            ctx.fail(new IllegalStateException("poll loop exhausted after " + MAX_SPINS + " iterations"));
        }
    }

    /**
     * The documented correct authoring: per-iteration step names from the deterministic loop counter. Each iteration
     * records its own wait (and, on timeout, its own sleep), so the loop genuinely re-registers and a late signal is
     * caught by the iteration parked at its arrival.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeCounterNames(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");

        try {
            boolean delivered = false;
            int spins = 0;
            while (!delivered && spins < MAX_SPINS) {
                spins++;
                effects.record(workflowId, EFFECT_ITERATION);

                var signal = ctx.waitForEvent(
                        STEP_POLL + "-" + spins,
                        PollSignalEvent.class,
                        associate(payloadProperty("orderId"), equalsTo(orderId)),
                        step -> step.timeout(POLL_TIMEOUT)
                );
                signal.await();
                if (signal.success()) {
                    delivered = true;
                } else {
                    ctx.sleep(STEP_RETRY_DELAY + "-" + spins, RETRY_DELAY);
                }
            }

            if (delivered) {
                process(ctx, workflowId);
            } else {
                ctx.fail(new IllegalStateException("poll loop exhausted after " + MAX_SPINS + " iterations"));
            }
        } catch (StepInterruptedException e) {
            throw e; // an engine interrupt is not a step failure: the step resumes on the next start
        } catch (StepFailedException e) {
            // Fuzz-workload hardening (OrderWorkflow pattern): a crash-resolved step failure (e.g. the process step
            // interrupted mid-flight) is propagated explicitly to terminal FAILED instead of the F-15 wedge sink.
            ctx.fail(e);
        }
    }

    private void process(SimpleWorkflowContext ctx, String workflowId) {
        ctx.awaitExecute(
                STEP_PROCESS,
                Map.of(),
                (pc, payload) -> Map.of("processed", effects.record(workflowId, STEP_PROCESS)),
                step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT)
        );
    }
}
