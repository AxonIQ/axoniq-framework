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
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;

import java.util.HashMap;
import java.util.Map;

/**
 * A workflow whose steps deterministically exercise <strong>all three</strong> payload reducers, used to exercise
 * INVARIANTS.md INV-19 ({@code PayloadReducerSemantics}): each payload reducer produces its documented merge into the
 * workflow payload, and that result is stable across crash/replay (axon-flow-workflow skill §5 "Step customization"
 * payload-reducers table; the three reducer types {@code GlobalOnlyPayloadReducer}/{@code CombineGlobalAndLocalPayloadReducer}/
 * {@code LocalOnlyPayloadReducer} and their {@code reduce(...)}/{@code NAME}; {@code EventSourcedWorkflowState#evolvePayload}).
 * It <strong>extends</strong> INV-13 ({@code NoLostPayloadWrites}, the {@link PayloadOrderWorkflow}): INV-13 covered
 * combine + local_only on the result side for no-lost-write; INV-19 pins each reducer <em>type</em>'s documented MERGE
 * semantics on both the parameter and result sides.
 * <p>
 * The body is a fixed, fully-deterministic {@code execute}/{@code modifyPayload} sequence (no external wait, no
 * randomness, no wall-clock — axon-flow-workflow skill §3.3), each step awaited in declaration order so the instance's
 * committed subsequence is a stable function of the history (so a replay rebuilds the identical payload — the
 * replay-stability facet INV-19 shares with INV-4 {@code DeterministicReplay}, and so it never trips the F-2
 * intra-instance append-order class). Each step's contribution is a constant, so the engine's reconstructed final
 * payload is known and the assertion is non-vacuous:
 * <ol>
 *   <li><strong>{@link #STEP_SEED} — {@code execute} + {@link CombineGlobalAndLocalPayloadReducer#INSTANCE}</strong>:
 *       merges {@link #KEY_SEED} into the payload. Establishes a prior key the {@code local_only} replace below must
 *       then DROP (so the replace's whole-payload-overwrite semantics is observable).</li>
 *   <li><strong>{@link #STEP_LOCAL_ONLY_REPLACE} — {@code modifyPayload}</strong> (the {@code local_only} reducer that
 *       <em>replaces</em> the whole payload): replaces the payload wholesale with {@code {}{@link #KEY_REPLACE}{@code }}
 *       <em>only</em>, so the prior {@link #KEY_SEED} is dropped. Demonstrates {@code LocalOnlyPayloadReducer}: the
 *       payload is REPLACED wholesale by the step's map.</li>
 *   <li><strong>{@link #STEP_COMBINE} — {@code execute} + {@link CombineGlobalAndLocalPayloadReducer#INSTANCE}</strong>:
 *       merges {@link #KEY_COMBINE} into the running payload (which already holds {@link #KEY_REPLACE}). Demonstrates
 *       {@code CombineGlobalAndLocalPayloadReducer}: the step's result map is MERGED key-by-key, keeping the existing
 *       key and adding its own — so {@link #KEY_COMBINE} MUST appear in the final payload.</li>
 *   <li><strong>{@link #STEP_GLOBAL_ONLY} — {@code execute} with the DEFAULT result reducer
 *       ({@link GlobalOnlyPayloadReducer})</strong>: returns a result map {@code {}{@link #KEY_GLOBAL_ONLY}{@code }} that
 *       the engine DISCARDS — the default {@code resultPayloadReducer} is {@code global_only}. Demonstrates
 *       {@code GlobalOnlyPayloadReducer}: the step's result is discarded, the payload is unchanged — so
 *       {@link #KEY_GLOBAL_ONLY} MUST be ABSENT from the final payload.</li>
 *   <li><strong>{@link #STEP_PARAM_VIEW} — {@code execute} +
 *       {@code parameterPayloadReducer(}{@link CombineGlobalAndLocalPayloadReducer#INSTANCE}{@code )}</strong>: the
 *       {@code parameterPayloadReducer} governs what the step's action SEES as its input payload (the step's local view)
 *       vs the global. Set to {@code combine_local_and_global}, the action's input payload = global ∪ local, so it SEES
 *       the {@link #KEY_COMBINE} key the combine step wrote into the global payload (the default {@code local_only}
 *       parameter reducer would NOT — it passes only the step's local input). The step records that observation as
 *       {@code {}{@link #KEY_PARAM_SAW_COMBINE}{@code =true}{@code }} via a combine result reducer, so the
 *       parameter-side view is observable in the final payload — it MUST be {@code true}.</li>
 *   <li><strong>{@link #STEP_INTERPLAY} — {@code execute} +
 *       {@code parameterPayloadReducer(combine)} + {@code resultPayloadReducer(}{@link GlobalOnlyPayloadReducer}{@code )}</strong>
 *       (edge (b): the parameter-view and result-write reducers are INDEPENDENT knobs): its action SEES the combined
 *       global payload (incl. {@link #KEY_COMBINE}) on the input side — so the parameter reducer governs the view — yet
 *       its result is written back under the DEFAULT-style {@code global_only} reducer and so is DISCARDED. It returns a
 *       result map {@code {}{@link #KEY_INTERPLAY}{@code }}, which the {@code global_only} write reducer drops: even
 *       though the action saw the combined input, nothing it returns lands. {@link #KEY_INTERPLAY} MUST be ABSENT from
 *       the final payload — the input view (combine) and the write-back (global_only) are orthogonal.</li>
 *   <li><strong>{@link #STEP_LWW_FIRST} then {@link #STEP_LWW_SECOND} — two {@code execute} + combine steps writing the
 *       SAME key</strong> {@link #KEY_LWW} (edge (c): last-writer-wins on the same key under combine): the first writes
 *       {@link #VALUE_LWW_FIRST}, the second (later in the per-instance step sequence — the deterministic order, NOT the
 *       global-append order) writes {@link #VALUE_LWW_SECOND}. Per the documented combine fold the later same-key write
 *       wins, so {@link #KEY_LWW} MUST equal {@link #VALUE_LWW_SECOND} in the final payload.</li>
 * </ol>
 * Edge (d) <strong>missing keys under combine</strong> rides the same body: every combine step's result map OMITS keys
 * the running payload already holds (e.g. {@link #STEP_COMBINE} omits {@link #KEY_REPLACE}), and per the documented
 * combine fold ({@code new HashMap<>(global); putAll(local)}) an omitted key keeps the running (global) value — so
 * {@link #KEY_REPLACE} survives every later combine that omits it. The deeper edge (a) <strong>a {@code null} value
 * under combine</strong> and the crash/replay-stability of all edges (e) are scenario-pinned in
 * {@code PayloadReducerSemanticsScenario}/{@code Inv19PayloadReducerSemanticsTest} (a {@code null} step value is
 * serialization-coupled, so it is observed/characterized in the deterministic scenario rather than folded into the
 * always-on fuzz instance, exactly so a serialization quirk cannot flake the 1000-seed sweep).
 * <p>
 * The engine's reconstructed final payload for the instance is therefore exactly
 * {@code {}{@link #KEY_REPLACE}{@code =replaced, }{@link #KEY_COMBINE}{@code =present, }{@link #KEY_PARAM_SAW_COMBINE}{@code =true, }{@link #KEY_LWW}{@code =second}{@code }}:
 * {@link #KEY_SEED} dropped (local_only replace), {@link #KEY_GLOBAL_ONLY} absent (global_only discard),
 * {@link #KEY_INTERPLAY} absent (parameter-combine input but global_only write → discarded), {@link #KEY_COMBINE}
 * present (combine merge), {@link #KEY_PARAM_SAW_COMBINE}={@code true} (parameter-side combine view), and {@link #KEY_LWW}
 * ={@link #VALUE_LWW_SECOND} (last-writer-wins on the same key). The assertion checks this matches the documented outcome
 * of each reducer against the engine's <em>own</em> reconstructed payload (the workflow-history read-model), so a reducer
 * mis-application or a replay that rebuilds a different payload would show.
 * <p>
 * The {@code execute} bodies also bump the shared {@link CountingEffects} counter (like the other simulation workflows)
 * so the run still exercises the effect-counting surface; INV-19 itself reads only the engine's reconstructed payload,
 * never the effect counters.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ReducerWorkflow {

    /**
     * Logical workflow name (single definition).
     */
    public static final String WORKFLOW_NAME = "ReducerWorkflow";

    /**
     * First step ({@code execute} + {@link CombineGlobalAndLocalPayloadReducer#INSTANCE}): merges {@link #KEY_SEED}, a
     * prior key the {@code local_only} replace below then drops.
     */
    public static final String STEP_SEED = "seedWrite";

    /**
     * Second step ({@code modifyPayload}, the {@code local_only} reducer that replaces the whole payload): replaces the
     * payload wholesale with {@link #KEY_REPLACE} only, dropping {@link #KEY_SEED}.
     */
    public static final String STEP_LOCAL_ONLY_REPLACE = "localOnlyReplace";

    /**
     * Third step ({@code execute} + {@link CombineGlobalAndLocalPayloadReducer#INSTANCE}): merges {@link #KEY_COMBINE}
     * into the running payload, keeping {@link #KEY_REPLACE}.
     */
    public static final String STEP_COMBINE = "combineWrite";

    /**
     * Fourth step ({@code execute} with the DEFAULT {@link GlobalOnlyPayloadReducer} result reducer): its result
     * {@link #KEY_GLOBAL_ONLY} is discarded — the payload is unchanged.
     */
    public static final String STEP_GLOBAL_ONLY = "globalOnlyWrite";

    /**
     * Fifth step ({@code execute} + {@code parameterPayloadReducer(combine)}): demonstrates the parameter-side input
     * view — its action sees the global payload (incl. {@link #KEY_COMBINE}) and records {@link #KEY_PARAM_SAW_COMBINE}.
     */
    public static final String STEP_PARAM_VIEW = "parameterViewWrite";

    /**
     * Sixth step ({@code execute} + {@code parameterPayloadReducer(combine)} + {@code resultPayloadReducer(global_only)}):
     * edge (b) — the parameter-view and result-write reducers are independent knobs. Its action SEES the combined global
     * payload, but its result is written back under {@code global_only} and DISCARDED — so {@link #KEY_INTERPLAY} never
     * lands.
     */
    public static final String STEP_INTERPLAY = "interplayWrite";

    /**
     * Seventh step ({@code execute} + combine): edge (c) — the FIRST writer of {@link #KEY_LWW} (value
     * {@link #VALUE_LWW_FIRST}); the later {@link #STEP_LWW_SECOND} overwrites it.
     */
    public static final String STEP_LWW_FIRST = "lastWriterFirst";

    /**
     * Eighth step ({@code execute} + combine): edge (c) — the LATER writer of {@link #KEY_LWW} (value
     * {@link #VALUE_LWW_SECOND}); per the documented combine fold this later same-key write wins.
     */
    public static final String STEP_LWW_SECOND = "lastWriterSecond";

    /**
     * Key written by {@link #STEP_SEED} (combine) — then DROPPED by the {@code local_only} replace; MUST be absent from
     * the final payload.
     */
    public static final String KEY_SEED = "seedKey";

    /**
     * Key the {@code local_only} replace ({@link #STEP_LOCAL_ONLY_REPLACE}) rewrites the whole payload to — MUST be the
     * only key surviving the replace (until later combine adds more); present in the final payload.
     */
    public static final String KEY_REPLACE = "replaceKey";

    /**
     * Value {@link #STEP_LOCAL_ONLY_REPLACE} writes under {@link #KEY_REPLACE}.
     */
    public static final String VALUE_REPLACE = "replaced";

    /**
     * Key written by {@link #STEP_COMBINE} (combine) — MUST be present in the final payload (merged, not discarded).
     */
    public static final String KEY_COMBINE = "combineKey";

    /**
     * Value {@link #STEP_COMBINE} writes under {@link #KEY_COMBINE}.
     */
    public static final String VALUE_COMBINE = "present";

    /**
     * Key {@link #STEP_GLOBAL_ONLY}'s result carries — but the default {@code global_only} result reducer DISCARDS it;
     * MUST be absent from the final payload.
     */
    public static final String KEY_GLOBAL_ONLY = "globalOnlyKey";

    /**
     * Key written by {@link #STEP_PARAM_VIEW} recording whether the parameter-side combine view let the action SEE the
     * {@link #KEY_COMBINE} key in the global payload — MUST be {@code true} in the final payload.
     */
    public static final String KEY_PARAM_SAW_COMBINE = "paramSawCombineKey";

    /**
     * Key {@link #STEP_INTERPLAY}'s result carries — written back under {@code global_only}, so it is DISCARDED despite
     * the step's combine parameter view; MUST be absent from the final payload (edge (b)).
     */
    public static final String KEY_INTERPLAY = "interplayKey";

    /**
     * Key written by both {@link #STEP_LWW_FIRST} and {@link #STEP_LWW_SECOND} (edge (c), last-writer-wins): MUST carry
     * the LATER writer's value {@link #VALUE_LWW_SECOND} in the final payload.
     */
    public static final String KEY_LWW = "lastWriterKey";

    /**
     * Value the FIRST writer ({@link #STEP_LWW_FIRST}) writes under {@link #KEY_LWW} — overwritten by the later writer.
     */
    public static final String VALUE_LWW_FIRST = "first";

    /**
     * Value the LATER writer ({@link #STEP_LWW_SECOND}) writes under {@link #KEY_LWW} — the winner per the documented
     * combine fold.
     */
    public static final String VALUE_LWW_SECOND = "second";

    /**
     * The null-edge body's combine step (edge (a)): merges {@link #KEY_NULL} (a {@code null} value) and
     * {@link #KEY_NULL_SIBLING} (a non-null sibling) under combine.
     */
    public static final String STEP_NULL_COMBINE = "nullCombineWrite";

    /**
     * Key the null-edge combine step writes with a {@code null} VALUE (edge (a)): characterizing how the engine's
     * {@code combine_local_and_global} reducer ({@code new HashMap<>(global); putAll(local)}) handles a null map value.
     */
    public static final String KEY_NULL = "nullValueKey";

    /**
     * A non-null key the same null-edge combine step writes — proves the step's other writes still land regardless of
     * the {@code null}-value sibling.
     */
    public static final String KEY_NULL_SIBLING = "nullSiblingKey";

    /**
     * Value the null-edge combine step writes under {@link #KEY_NULL_SIBLING}.
     */
    public static final String VALUE_NULL_SIBLING = "sibling";

    /**
     * The throwing-modifier body's {@code modifyPayload} step (candidate finding F-6, S-4 generalization): a
     * {@code modifyPayload} step whose modifier lambda throws a plain {@link RuntimeException} between primitives.
     */
    public static final String STEP_THROWING_MODIFIER = "throwingModifier";

    /**
     * The message the throwing-modifier lambda throws (S-4): a plain {@link RuntimeException} — NOT a
     * {@code WorkflowFailedException}/{@code WorkflowCancelledException}/timeout/{@code WorkflowReplayDriftException},
     * so it lands in {@code handleWorkflowException}'s {@code default} branch (no terminal status recorded).
     */
    public static final String THROWING_MODIFIER_BOOM = "S-4 boom: a plain RuntimeException from a modifyPayload modifier";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every {@code execute} body bumps a counter here.
     */
    public ReducerWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The body: a fixed deterministic sequence exercising all three reducers and the reducer EDGE cases — a combine
     * seed, a {@code local_only} replace that drops the seed, a combine that must appear, a {@code global_only} step
     * whose result must be discarded, a {@code parameterPayloadReducer(combine)} step that must SEE the combined global
     * payload, an interplay step whose combine parameter view but {@code global_only} write means its result is
     * discarded (edge (b), independent knobs), and a last-writer-wins same-key combine pair (edge (c)). Edge (d)
     * missing-key-under-combine rides the same body (each combine omits keys the payload already holds, which survive).
     * Each step is awaited in declaration order so the committed subsequence is a stable function of the history; the
     * instance completes on its own (no external wait).
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();

        // 1) combine seed — establishes a prior key the local_only replace then drops.
        ctx.awaitExecute(
                STEP_SEED,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_SEED);
                    return Map.of(KEY_SEED, true);
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));

        // 2) local_only replace (modifyPayload): rewrite the WHOLE payload to {replaceKey} only — drops seedKey.
        // Deterministic: returns a constant map, ignoring the current payload, so the replace is total and known.
        ctx.awaitModifyPayload(
                STEP_LOCAL_ONLY_REPLACE,
                currentPayload -> {
                    var replacement = new HashMap<String, Object>();
                    replacement.put(KEY_REPLACE, VALUE_REPLACE);
                    return replacement;
                });

        // 3) combine — merges combineKey into the running payload (keeps replaceKey). Must appear in the final payload.
        ctx.awaitExecute(
                STEP_COMBINE,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_COMBINE);
                    return Map.of(KEY_COMBINE, VALUE_COMBINE);
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));

        // 4) global_only — DEFAULT result reducer (GlobalOnlyPayloadReducer): the result is discarded. globalOnlyKey
        // must NOT appear in the final payload. (No resultPayloadReducer customization → the default global_only.)
        ctx.awaitExecute(
                STEP_GLOBAL_ONLY,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_GLOBAL_ONLY);
                    return Map.of(KEY_GLOBAL_ONLY, "discarded");
                });

        // 5) parameterPayloadReducer(combine) — the action's INPUT view = global ∪ local, so it SEES combineKey (written
        // into the global payload by step 3). The default local_only parameter reducer would NOT (it passes only the
        // step's local input). Record the observation (combine result reducer so it lands in the engine's payload).
        ctx.awaitExecute(
                STEP_PARAM_VIEW,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_PARAM_VIEW);
                    boolean sawCombineKey = VALUE_COMBINE.equals(payload.get(KEY_COMBINE));
                    return Map.of(KEY_PARAM_SAW_COMBINE, sawCombineKey);
                },
                step -> step.parameterPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE)
                            .resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));

        // 6) parameter-view vs result-write interplay (edge (b)): the action SEES the combined global payload (combine
        // parameter reducer) but its result is written back under global_only (the DEFAULT-style result reducer) and so
        // is DISCARDED. The two reducers are independent knobs: a combined INPUT view does NOT imply a combined
        // WRITE-back. interplayKey must NOT appear in the final payload.
        ctx.awaitExecute(
                STEP_INTERPLAY,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_INTERPLAY);
                    // It can read combineKey from the global payload (proving the input view) — but whatever it returns
                    // is discarded by the global_only write reducer below.
                    return Map.of(KEY_INTERPLAY, VALUE_COMBINE.equals(payload.get(KEY_COMBINE)));
                },
                step -> step.parameterPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE)
                            .resultPayloadReducer(GlobalOnlyPayloadReducer.INSTANCE));

        // 7) + 8) last-writer-wins on the same key (edge (c)): two combine steps both write KEY_LWW. The LATER one (in
        // this per-instance step sequence — the deterministic order, NOT the non-deterministic global-append order)
        // wins per the documented combine fold (new HashMap<>(global); putAll(local)). lastWriterKey must equal "second".
        ctx.awaitExecute(
                STEP_LWW_FIRST,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_LWW_FIRST);
                    return Map.of(KEY_LWW, VALUE_LWW_FIRST);
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));
        ctx.awaitExecute(
                STEP_LWW_SECOND,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_LWW_SECOND);
                    return Map.of(KEY_LWW, VALUE_LWW_SECOND);
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));
    }

    /**
     * The null-value edge body (edge (a) of INV-19): a single {@code execute} + combine step whose result map carries a
     * {@code null} value under {@link #KEY_NULL} alongside a non-null {@link #KEY_NULL_SIBLING}. The engine's
     * {@code combine_local_and_global} reducer is {@code new HashMap<>(global); putAll(local)}, and {@code HashMap.putAll}
     * inserts a {@code null} value (a {@code HashMap} permits null values) — so the DOCUMENTED behaviour is that the key
     * is PRESENT with value {@code null} (combine OVERWRITES with null; it does NOT skip it nor keep a prior global
     * value). The scenario/test characterizes the engine's ACTUAL behaviour against this documented fold via the
     * content-based {@code (A) == (B)} oracle and {@code flags} any divergence (e.g. if serialization were to drop the
     * null) rather than patching anything. The sibling key proves the step's other writes still land.
     * <p>
     * Built with a mutable {@link HashMap} (not {@link Map#of}, which rejects {@code null} values) so the {@code null}
     * value reaches the reducer.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void nullEdge(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        ctx.awaitExecute(
                STEP_NULL_COMBINE,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_NULL_COMBINE);
                    var result = new HashMap<String, Object>();
                    result.put(KEY_NULL, null);                 // a null VALUE under combine — the edge (a) probe.
                    result.put(KEY_NULL_SIBLING, VALUE_NULL_SIBLING);
                    return result;
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));
    }

    /**
     * The throwing-modifier body (candidate finding F-6, S-4 generalization): a single {@code modifyPayload} step whose
     * modifier lambda throws a plain {@link RuntimeException} ({@link #THROWING_MODIFIER_BOOM}) between primitives — the
     * clearest trigger of the {@code handleWorkflowException} {@code default}-branch wedge.
     * <p>
     * The modifier lambda runs ON the workflow thread inside {@code PayloadDelegate.modifyPayload}'s {@code appendTask}
     * task ({@code payloadModification.apply(...)}, {@code PayloadDelegate.java:86}), whose {@code .join()}
     * ({@code PayloadDelegate.java:98}) re-throws on the task-queue thread; {@code awaitStateChange}
     * ({@code SimpleWorkflowExecution.java:348-351}) propagates it up out of {@code modifyPayload} → the workflow body →
     * {@code executeWorkflow} → the {@code try/catch (Throwable)} ({@code SimpleWorkflowExecution.java:160-164}) →
     * {@code handleWorkflowException}. Because a plain {@code RuntimeException} matches none of the typed branches
     * (WorkflowFailed/WorkflowCancelled/Timeout/WorkflowReplayDrift/Interrupted), it lands in the {@code default} branch
     * ({@code SimpleWorkflowExecution.java:311-323}), which logs the error and deliberately records NO terminal status
     * ("we agreed not to drive the workflow to terminal state on any other exception"). So the instance is left
     * <strong>non-terminal / stuck</strong> (a liveness stall), the exception merely logged; replay re-runs the body and
     * re-hits the same throw (recovery-unsafe). This is the SAME default-branch sink as F-6's null-payload trigger, with a
     * wider trigger family — characterized and FLAGGED, not patched (per the POC's test/docs-only rule).
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void throwingModifier(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        ctx.awaitModifyPayload(
                STEP_THROWING_MODIFIER,
                currentPayload -> {
                    effects.record(workflowId, STEP_THROWING_MODIFIER);
                    // A plain RuntimeException thrown by a user modifier lambda BETWEEN primitives — the S-4 trigger.
                    throw new RuntimeException(THROWING_MODIFIER_BOOM);
                });
    }
}
