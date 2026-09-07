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
package io.axoniq.framework.workflow.simulation;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.EventNameCustomizationSoundScenario;
import io.axoniq.framework.workflow.simulation.workflow.CustomNamedWorkflow;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises INVARIANTS.md INV-22 ({@code EventNameCustomizationSound}): a workflow registered with a custom
 * {@code eventNameCustomizer} records its step/status events under the CUSTOMIZED wire names, those names are stable
 * across crash/replay (a pure function of history), and the engine still routes/replays correctly under customization.
 * <p>
 * The first test drives the real engine through {@link CustomNamedWorkflow} registered with its custom customizer (a
 * custom namespace + workflow base name + a {@code stepCompleted("Done")} status-suffix override): a fresh start runs
 * the body to COMPLETED recording the customized names, and a crash + replay reproduces the IDENTICAL customized names.
 * The remaining tests are hand-built assertion pins proving {@link Invariants#assertEventNameCustomizationSound} is
 * correct and not trivial: the good case (the expected customized names) passes; an un-customized namespace throws; a
 * default {@code Completed} suffix where the override dictates {@code Done} throws; a missing start (routed to none)
 * throws; a present-but-never-terminal instance throws; and an out-of-scope (non-{@code named-}) instance is skipped.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv22EventNameCustomizationSoundTest {

    private static final String WORKFLOW_ID = "named-A";

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void freshStart_recordsCustomizedNames_reachesTerminal_identicalAcrossReplay() {
        EventNameCustomizationSoundScenario.Outcome outcome = EventNameCustomizationSoundScenario.run(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("CustomNamedWorkflow must reach a terminal (COMPLETED) workflow status under customization")
                .isTrue();
        // The customized names applied: under the custom namespace, the customizer-derived local names for each step
        // (STARTED + the overridden Done COMPLETED) and the workflow STARTED/COMPLETED status events.
        assertThat(outcome.customizedNamesBeforeCrash())
                .as("EventNameCustomizationSound: every committed event carries its customizer-derived wire name")
                .contains(fullName(CustomNamedWorkflow.expectedStepLocalName(
                                  CustomNamedWorkflow.STEP_PREPARE_ORDER, CustomNamedWorkflow.STEP_STARTED_SUFFIX)),
                          fullName(CustomNamedWorkflow.expectedStepLocalName(
                                  CustomNamedWorkflow.STEP_PREPARE_ORDER, CustomNamedWorkflow.STEP_COMPLETED_SUFFIX)),
                          fullName(CustomNamedWorkflow.expectedStepLocalName(
                                  CustomNamedWorkflow.STEP_FINALIZE_ORDER, CustomNamedWorkflow.STEP_STARTED_SUFFIX)),
                          fullName(CustomNamedWorkflow.expectedStepLocalName(
                                  CustomNamedWorkflow.STEP_FINALIZE_ORDER, CustomNamedWorkflow.STEP_COMPLETED_SUFFIX)),
                          fullName(CustomNamedWorkflow.expectedWorkflowLocalName(
                                  CustomNamedWorkflow.WORKFLOW_STARTED_SUFFIX)),
                          fullName(CustomNamedWorkflow.expectedWorkflowLocalName(
                                  CustomNamedWorkflow.WORKFLOW_COMPLETED_SUFFIX)));
        // The overridden suffix is genuinely applied: a step COMPLETED ends in "Done", never the default "Completed".
        assertThat(outcome.customizedNamesBeforeCrash())
                .as("the stepCompleted override is applied: step COMPLETED carries the 'Done' suffix, not 'Completed'")
                .doesNotContain(fullName(CustomNamedWorkflow.expectedStepLocalName(
                        CustomNamedWorkflow.STEP_PREPARE_ORDER, "Completed")));
        // Stable across replay: a crash + replay reproduces the IDENTICAL customized names.
        assertThat(outcome.customizedNamesAfterCrash())
                .as("EventNameCustomizationSound: replay reproduces the identical customized names")
                .isEqualTo(outcome.customizedNamesBeforeCrash());
    }

    @Test
    void assertEventNameCustomizationSound_passesForCorrectCustomizedNames() {
        // One custom-named instance with exactly the customizer-derived names, reaching a terminal status. Sound.
        List<EventMessage> log = correctlyNamedInstance();

        assertThatCode(() -> Invariants.assertEventNameCustomizationSound(log, "named-", Set.of(WORKFLOW_ID)))
                .as("a single instance carrying the customizer-derived names + terminal is sound")
                .doesNotThrowAnyException();
    }

    @Test
    void assertEventNameCustomizationSound_detectsUnCustomizedNamespace() {
        // The genuine break: a step event carries the DEFAULT namespace instead of the custom one — the customizer was
        // not applied. A real customization failure. Pins that the assertion catches it.
        List<EventMessage> log = List.of(
                workflowStatusEvent(CustomNamedWorkflow.NAMESPACE,
                                    CustomNamedWorkflow.expectedWorkflowLocalName(
                                            CustomNamedWorkflow.WORKFLOW_STARTED_SUFFIX),
                                    WorkflowStatus.STARTED),
                // illegal: the engine default namespace, not the registered custom namespace.
                stepEvent("io.axoniq.framework.workflow",
                          CustomNamedWorkflow.expectedStepLocalName(
                                  CustomNamedWorkflow.STEP_PREPARE_ORDER, CustomNamedWorkflow.STEP_STARTED_SUFFIX),
                          CustomNamedWorkflow.STEP_PREPARE_ORDER, StepStatus.STARTED));

        assertThatThrownBy(() -> Invariants.assertEventNameCustomizationSound(log, "named-", Set.of()))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("EventNameCustomizationSound")
                .hasMessageContaining(WORKFLOW_ID)
                .hasMessageContaining("namespace");
    }

    @Test
    void assertEventNameCustomizationSound_detectsDefaultSuffixWhereOverrideDictatesDone() {
        // The genuine break: a step COMPLETED event carries the DEFAULT 'Completed' suffix where the registered
        // customizer's stepCompleted("Done") override dictates 'Done' — the override was not applied.
        List<EventMessage> log = List.of(
                workflowStatusEvent(CustomNamedWorkflow.NAMESPACE,
                                    CustomNamedWorkflow.expectedWorkflowLocalName(
                                            CustomNamedWorkflow.WORKFLOW_STARTED_SUFFIX),
                                    WorkflowStatus.STARTED),
                // illegal: 'PrepareOrderCompleted' instead of the override's 'PrepareOrderDone'.
                stepEvent(CustomNamedWorkflow.NAMESPACE,
                          CustomNamedWorkflow.expectedStepLocalName(CustomNamedWorkflow.STEP_PREPARE_ORDER, "Completed"),
                          CustomNamedWorkflow.STEP_PREPARE_ORDER, StepStatus.COMPLETED));

        assertThatThrownBy(() -> Invariants.assertEventNameCustomizationSound(log, "named-", Set.of()))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("EventNameCustomizationSound")
                .hasMessageContaining(WORKFLOW_ID)
                .hasMessageContaining("PrepareOrderDone");
    }

    @Test
    void assertEventNameCustomizationSound_detectsDroppedStart_routedToNone() {
        // The complementary break: the harness KNOWS it started named-A, but the committed log contains NO event for it
        // — the start produced no instance (routed to no definition under customization).
        List<EventMessage> log = List.of();

        assertThatThrownBy(() -> Invariants.assertEventNameCustomizationSound(log, "named-", Set.of(WORKFLOW_ID)))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("EventNameCustomizationSound")
                .hasMessageContaining(WORKFLOW_ID)
                .hasMessageContaining("routed to NO definition");
    }

    @Test
    void assertEventNameCustomizationSound_detectsNeverTerminal() {
        // The instance is present with correctly-customized names but never recorded a terminal status — under
        // customization the engine must still complete the workflow; a never-terminal instance is a regression.
        List<EventMessage> log = List.of(
                workflowStatusEvent(CustomNamedWorkflow.NAMESPACE,
                                    CustomNamedWorkflow.expectedWorkflowLocalName(
                                            CustomNamedWorkflow.WORKFLOW_STARTED_SUFFIX),
                                    WorkflowStatus.STARTED),
                stepEvent(CustomNamedWorkflow.NAMESPACE,
                          CustomNamedWorkflow.expectedStepLocalName(
                                  CustomNamedWorkflow.STEP_PREPARE_ORDER, CustomNamedWorkflow.STEP_STARTED_SUFFIX),
                          CustomNamedWorkflow.STEP_PREPARE_ORDER, StepStatus.STARTED));

        assertThatThrownBy(() -> Invariants.assertEventNameCustomizationSound(log, "named-", Set.of(WORKFLOW_ID)))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("EventNameCustomizationSound")
                .hasMessageContaining(WORKFLOW_ID)
                .hasMessageContaining("never recorded a terminal workflow status");
    }

    @Test
    void assertEventNameCustomizationSound_skipsOutOfScopeInstances() {
        // A non-custom-named instance (different prefix) is not constrained by INV-22 — must be skipped, not flagged,
        // even though its events carry the default (non-custom) namespace.
        assertThatCode(() -> Invariants.assertEventNameCustomizationSound(orderInstance(), "named-", Set.of()))
                .as("non-custom-named (order-) instances are out of INV-22's scope and are skipped")
                .doesNotThrowAnyException();
    }

    /**
     * A correctly-customized + terminal custom-named instance: the customized workflow STARTED + COMPLETED, and a
     * customized STARTED + overridden Done COMPLETED for one step, all under the custom namespace.
     */
    private static List<EventMessage> correctlyNamedInstance() {
        return List.of(
                workflowStatusEvent(CustomNamedWorkflow.NAMESPACE,
                                    CustomNamedWorkflow.expectedWorkflowLocalName(
                                            CustomNamedWorkflow.WORKFLOW_STARTED_SUFFIX),
                                    WorkflowStatus.STARTED),
                stepEvent(CustomNamedWorkflow.NAMESPACE,
                          CustomNamedWorkflow.expectedStepLocalName(
                                  CustomNamedWorkflow.STEP_PREPARE_ORDER, CustomNamedWorkflow.STEP_STARTED_SUFFIX),
                          CustomNamedWorkflow.STEP_PREPARE_ORDER, StepStatus.STARTED),
                stepEvent(CustomNamedWorkflow.NAMESPACE,
                          CustomNamedWorkflow.expectedStepLocalName(
                                  CustomNamedWorkflow.STEP_PREPARE_ORDER, CustomNamedWorkflow.STEP_COMPLETED_SUFFIX),
                          CustomNamedWorkflow.STEP_PREPARE_ORDER, StepStatus.COMPLETED),
                workflowStatusEvent(CustomNamedWorkflow.NAMESPACE,
                                    CustomNamedWorkflow.expectedWorkflowLocalName(
                                            CustomNamedWorkflow.WORKFLOW_COMPLETED_SUFFIX),
                                    WorkflowStatus.COMPLETED));
    }

    /**
     * A single-version order instance (different prefix, default namespace) — out of INV-22's scope.
     */
    private static List<EventMessage> orderInstance() {
        return List.of(
                new GenericEventMessage(
                        new MessageType(new QualifiedName("io.axoniq.framework.workflow", "OrderWorkflowStarted"),
                                        MessageType.DEFAULT_VERSION),
                        Map.of(), MetadataUtils.create("order-wf0", WorkflowStatus.STARTED)),
                new GenericEventMessage(
                        new MessageType(new QualifiedName("io.axoniq.framework.workflow", "ReserveInventoryStarted"),
                                        MessageType.DEFAULT_VERSION),
                        Map.of(), MetadataUtils.create("order-wf0", "reserveInventory", StepStatus.STARTED)));
    }

    private static String fullName(String localName) {
        return new QualifiedName(CustomNamedWorkflow.NAMESPACE, localName).fullName();
    }

    private static EventMessage stepEvent(String namespace, String localName, String stepName, StepStatus status) {
        Metadata metadata = MetadataUtils.create(WORKFLOW_ID, stepName, status);
        return new GenericEventMessage(new MessageType(new QualifiedName(namespace, localName),
                                                       MessageType.DEFAULT_VERSION), Map.of(), metadata);
    }

    private static EventMessage workflowStatusEvent(String namespace, String localName, WorkflowStatus status) {
        Metadata metadata = MetadataUtils.create(WORKFLOW_ID, status);
        return new GenericEventMessage(new MessageType(new QualifiedName(namespace, localName),
                                                       MessageType.DEFAULT_VERSION), Map.of(), metadata);
    }
}
