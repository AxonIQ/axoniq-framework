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
import io.axoniq.framework.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;

import java.util.Map;

/**
 * A workflow registered with a custom {@link EventNameCustomizer}, used to exercise INVARIANTS.md INV-22
 * ({@code EventNameCustomizationSound}). It is a pure {@code execute} sequence (two counting steps then completes), so
 * the instance reaches a terminal (COMPLETED) status on its own — keeping the harness's liveness assertion simple while
 * still driving the customized-name surface through the crash/restart/reorder faults.
 * <p>
 * The customizer ({@link #customizer()}) is registered on the workflow (via {@code WorkflowCustomization.eventNameCustomizer}
 * inside {@code .customized(...)}, the same registration hook INV-17 uses for its status-change listener), so it is
 * <strong>inherited per step</strong> ({@code EventNameCustomizer.forStepInheritance}) and the engine names every
 * emitted step/status event through it ({@code EventMessageUtils} calls {@code customizer.getEventName(...)} and stamps
 * the result on the event's {@code MessageType} / {@code QualifiedName}). The customizer is deterministic and its rules
 * are fixed here, so the EXPECTED wire name of every event is known up front (see {@link #expectedStepLocalName} /
 * {@link #expectedWorkflowLocalName} and {@link #NAMESPACE}) — which is what makes the INV-22 assertion non-vacuous:
 * the committed events' types/names must EQUAL these derived expected names, not merely be non-empty.
 * <p>
 * Customizer rules (a {@link DefaultEventNameCustomizer}, see axon-flow-workflow skill §6.2):
 * <ul>
 *   <li>a custom {@link #NAMESPACE} (overriding the {@code io.axoniq.framework.workflow} default) — lands on
 *       {@code QualifiedName.namespace()};</li>
 *   <li>a {@link #WORKFLOW_BASE_NAME workflowBaseName} (used for the workflow-status events' base);</li>
 *   <li>a single status-suffix override: {@code stepCompleted("Done")} (the default step-completed suffix is
 *       {@code "Completed"}).</li>
 * </ul>
 * Because {@code baseName} is left unset, a step event's base is the (capitalized) step name itself; because the
 * workflow-status events use {@code workflowBaseName}, they are named from {@link #WORKFLOW_BASE_NAME}. The status
 * suffixes are the {@code DefaultEventNameCustomizer} defaults except the one override, and {@code capitalizeSimpleName}
 * stays at its default {@code true}. So, for step {@code prepareOrder}: {@code STARTED} &rarr;
 * {@code PrepareOrderStarted}, {@code COMPLETED} &rarr; {@code PrepareOrderDone}; and the workflow status events are
 * {@code CustomNamedStarted} (STARTED) and {@code CustomNamedCompleted} (COMPLETED) — the workflow COMPLETED suffix is
 * NOT the {@code stepCompleted} override (that override is step-status only).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class CustomNamedWorkflow {

    /**
     * Logical workflow name, reused by the harness when registering the definition. (Distinct from the customized wire
     * names: the logical name is the registration key; the wire names are what the customizer produces on the events.)
     */
    public static final String WORKFLOW_NAME = "CustomNamedWorkflow";

    /**
     * Custom namespace the customizer stamps on every emitted event's {@code QualifiedName.namespace()}, overriding the
     * {@code DefaultEventNameCustomizer} default ({@code io.axoniq.framework.workflow}).
     */
    public static final String NAMESPACE = "io.axoniq.framework.workflow.simulation.named";

    /**
     * The {@code workflowBaseName} the customizer uses to name the workflow-status events (STARTED/COMPLETED/...). Step
     * events do not use it (their base is the capitalized step name, since no step {@code baseName} is set).
     */
    public static final String WORKFLOW_BASE_NAME = "CustomNamed";

    /**
     * The overridden step-COMPLETED status suffix ({@code stepCompleted("Done")}; default is {@code "Completed"}). The
     * one status-suffix override the customizer carries, so a step COMPLETED event ends in {@code Done} rather than
     * {@code Completed}.
     */
    public static final String STEP_COMPLETED_SUFFIX = "Done";

    /**
     * The (default) step-STARTED status suffix the customizer carries unchanged.
     */
    public static final String STEP_STARTED_SUFFIX = "Started";

    /**
     * The (default) workflow-STARTED status suffix the customizer carries unchanged.
     */
    public static final String WORKFLOW_STARTED_SUFFIX = "Started";

    /**
     * The (default) workflow-COMPLETED status suffix the customizer carries unchanged (NOT the {@code stepCompleted}
     * override — that override is step-status only).
     */
    public static final String WORKFLOW_COMPLETED_SUFFIX = "Completed";

    /**
     * First step (a counting side effect).
     */
    public static final String STEP_PREPARE_ORDER = "prepareOrder";

    /**
     * Second step (a counting side effect), giving a real {@code execute}&rarr;{@code execute} sequence after the start.
     */
    public static final String STEP_FINALIZE_ORDER = "finalizeOrder";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every {@code execute} body bumps a counter here.
     */
    public CustomNamedWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Builds the deterministic custom {@link EventNameCustomizer} this workflow registers: a custom namespace + workflow
     * base name + a single {@code stepCompleted} status-suffix override. Deterministic and fixed, so the expected wire
     * names are knowable (see the class Javadoc and {@link #expectedStepLocalName}/{@link #expectedWorkflowLocalName}).
     *
     * @return the custom event-name customizer.
     */
        public static EventNameCustomizer customizer() {
        return DefaultEventNameCustomizer.Builder.namespace(NAMESPACE)
                                                 .workflowBaseName(WORKFLOW_BASE_NAME)
                                                 .stepCompleted(STEP_COMPLETED_SUFFIX);
    }

    /**
     * The EXPECTED customized local name of a step event, derived from the customizer's rules: the capitalized step name
     * (no step {@code baseName} is set) followed by the status suffix.
     *
     * @param stepName the step name (e.g. {@code prepareOrder}).
     * @param suffix   the status suffix (e.g. {@link #STEP_STARTED_SUFFIX} or {@link #STEP_COMPLETED_SUFFIX}).
     * @return the expected {@code QualifiedName.localName()} (e.g. {@code PrepareOrderStarted} / {@code PrepareOrderDone}).
     */
        public static String expectedStepLocalName(String stepName, String suffix) {
        return capitalize(stepName) + suffix;
    }

    /**
     * The EXPECTED customized local name of a workflow-status event, derived from the customizer's rules: the
     * (capitalized) {@link #WORKFLOW_BASE_NAME} followed by the workflow-status suffix.
     *
     * @param suffix the workflow-status suffix (e.g. {@link #WORKFLOW_STARTED_SUFFIX} or
     *               {@link #WORKFLOW_COMPLETED_SUFFIX}).
     * @return the expected {@code QualifiedName.localName()} (e.g. {@code CustomNamedStarted} / {@code CustomNamedCompleted}).
     */
        public static String expectedWorkflowLocalName(String suffix) {
        return capitalize(WORKFLOW_BASE_NAME) + suffix;
    }

    /**
     * Capitalizes the first character (matching {@code DefaultEventNameCustomizer}'s {@code capitalizeSimpleName=true}
     * default, which delegates to {@code org.axonframework.common.StringUtils.capitalize}).
     */
        private static String capitalize(String value) {
        if (value.isEmpty()) {
            return value;
        }
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }

    /**
     * Workflow body: two {@code execute} steps then completes. Each body increments the matching {@link CountingEffects}
     * counter so the harness can confirm the instance ran; the steps' events are named by the custom customizer.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        ctx.awaitExecute(STEP_PREPARE_ORDER, Map.of(),
                         (pc, payload) -> Map.of("prepared", effects.record(workflowId, STEP_PREPARE_ORDER)));
        ctx.awaitExecute(STEP_FINALIZE_ORDER, Map.of(),
                         (pc, payload) -> Map.of("finalized", effects.record(workflowId, STEP_FINALIZE_ORDER)));
    }
}
