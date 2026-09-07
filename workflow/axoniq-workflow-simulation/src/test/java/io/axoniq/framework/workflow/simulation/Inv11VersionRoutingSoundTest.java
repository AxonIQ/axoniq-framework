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
import io.axoniq.framework.workflow.simulation.scenarios.VersionRoutingSoundScenario;
import io.axoniq.framework.workflow.simulation.workflow.VersionedOrderWorkflow;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
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
 * Exercises INVARIANTS.md INV-11 ({@code VersionRoutingSound}): with two versions of one workflow registered, a fresh
 * start routes to exactly one definition at the highest registered version, and that version resolution is
 * deterministic across replay.
 * <p>
 * The first test drives the real engine through {@link VersionedOrderWorkflow} registered at v1.0.0 + v1.0.1: a fresh
 * start spawns at the highest version (v2), runs the v2-only step (never the v1-only step), and resolves to exactly one
 * version; a crash + replay resolves the SAME version. The remaining tests are assertion pins proving
 * {@link Invariants#assertVersionRoutingSound} is correct and not trivial: a single correct resolution passes; a history
 * where one instance's events carry TWO distinct versions (routed to 2 definitions) throws; a started instance whose
 * events carry NO resolvable version (routed to 0) throws; a fresh spawn at a non-highest version throws; and the
 * cross-instance interleaving of the global log (the F-2 surface, asserted per {@code workflowId}) is tolerated.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv11VersionRoutingSoundTest {

    private static final String HIGHEST = VersionedOrderWorkflow.VERSION_V2; // 1.0.1
    private static final String LOWER = VersionedOrderWorkflow.VERSION_V1;   // 1.0.0

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void freshStart_spawnsAtHighestVersion_routesToExactlyOneDefinition_deterministicAcrossReplay() {
        VersionRoutingSoundScenario.Outcome outcome = VersionRoutingSoundScenario.run(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("VersionedOrderWorkflow must reach a terminal (COMPLETED) workflow status")
                .isTrue();
        // Exactly one definition handled it — the highest registered version (a fresh start spawns there).
        assertThat(outcome.resolvedVersionsBeforeCrash())
                .as("VersionRoutingSound: a fresh start routes to exactly one definition, at the highest version")
                .containsExactly(HIGHEST);
        assertThat(outcome.ranV2OnlyStep())
                .as("the v2 body ran (highest version) — its v2-only step is recorded")
                .isTrue();
        assertThat(outcome.ranV1OnlyStep())
                .as("the v1 body must NOT run for a fresh start — its v1-only step must be absent")
                .isFalse();
        // Deterministic across replay: a crash + replay resolves the SAME single version.
        assertThat(outcome.resolvedVersionsAfterCrash())
                .as("VersionRoutingSound: replaying the same history resolves the same version for the instance")
                .isEqualTo(outcome.resolvedVersionsBeforeCrash())
                .containsExactly(HIGHEST);
    }

    @Test
    void assertVersionRoutingSound_passesForSingleCorrectResolution() {
        // One versioned instance: STARTED + a step, all at the highest version. Exactly one definition, highest version.
        List<EventMessage> log = List.of(
                workflowStatus("vorder-wf0", WorkflowStatus.STARTED, HIGHEST),
                step("vorder-wf0", "reserveInventory", StepStatus.COMPLETED, HIGHEST),
                step("vorder-wf0", VersionedOrderWorkflow.STEP_PROCESS_V2, StepStatus.COMPLETED, HIGHEST),
                workflowStatus("vorder-wf0", WorkflowStatus.COMPLETED, HIGHEST));

        assertThatCode(() -> Invariants.assertVersionRoutingSound(log, "vorder-", HIGHEST, Set.of("vorder-wf0")))
                .as("a single instance resolved to exactly the highest version is sound")
                .doesNotThrowAnyException();
    }

    @Test
    void assertVersionRoutingSound_detectsTwoVersionsHandlingOneInstance() {
        // The genuine break: the SAME instance's committed events carry TWO distinct versions — two definitions both
        // drove it (routed to 2). A real routing failure. Pins that the assertion catches it.
        List<EventMessage> log = List.of(
                workflowStatus("vorder-wf0", WorkflowStatus.STARTED, HIGHEST),
                step("vorder-wf0", "reserveInventory", StepStatus.COMPLETED, HIGHEST),
                // illegal: a second definition (the lower version) also emitted a step for this same instance.
                step("vorder-wf0", VersionedOrderWorkflow.STEP_CHARGE_V1, StepStatus.COMPLETED, LOWER));

        assertThatThrownBy(() -> Invariants.assertVersionRoutingSound(log, "vorder-", HIGHEST, Set.of("vorder-wf0")))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("VersionRoutingSound")
                .hasMessageContaining("vorder-wf0")
                .hasMessageContaining("routed to 2");
    }

    @Test
    void assertVersionRoutingSound_detectsDroppedStart_routedToZero() {
        // The complementary break: the harness KNOWS it started vorder-wf0, but the committed log contains NO event for
        // it — the start event produced no instance (routed to 0 definitions). An empty log with the id in the
        // expected-started set models exactly that.
        List<EventMessage> log = List.of();

        assertThatThrownBy(() -> Invariants.assertVersionRoutingSound(log, "vorder-", HIGHEST, Set.of("vorder-wf0")))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("VersionRoutingSound")
                .hasMessageContaining("vorder-wf0")
                .hasMessageContaining("routed to 0");
    }

    @Test
    void assertVersionRoutingSound_detectsFreshSpawnNotAtHighestVersion() {
        // A fresh spawn resolved to the LOWER version instead of the highest — a new instance must spawn at the highest.
        List<EventMessage> log = List.of(
                workflowStatus("vorder-wf0", WorkflowStatus.STARTED, LOWER),
                step("vorder-wf0", VersionedOrderWorkflow.STEP_CHARGE_V1, StepStatus.COMPLETED, LOWER));

        assertThatThrownBy(() -> Invariants.assertVersionRoutingSound(log, "vorder-", HIGHEST, Set.of("vorder-wf0")))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("VersionRoutingSound")
                .hasMessageContaining("highest registered version");
    }

    @Test
    void assertVersionRoutingSound_toleratesCrossInstanceDifferentVersions() {
        // Two INDEPENDENT versioned instances, each soundly at the highest version, interleaved in the global log.
        // INV-11 is per-instance, so wf1's events are not "a second version for wf0" — must pass.
        List<EventMessage> log = List.of(
                workflowStatus("vorder-wf0", WorkflowStatus.STARTED, HIGHEST),
                workflowStatus("vorder-wf1", WorkflowStatus.STARTED, HIGHEST),
                step("vorder-wf0", "reserveInventory", StepStatus.COMPLETED, HIGHEST),
                step("vorder-wf1", "reserveInventory", StepStatus.COMPLETED, HIGHEST));

        assertThatCode(() -> Invariants.assertVersionRoutingSound(log, "vorder-", HIGHEST, Set.of()))
                .as("independent instances each at the highest version are sound (per-workflowId)")
                .doesNotThrowAnyException();
    }

    @Test
    void assertVersionRoutingSound_skipsNonVersionedInstances() {
        // A single-version order instance (different prefix) is not constrained by INV-11 — must be skipped, not flagged.
        List<EventMessage> log = List.of(
                workflowStatus("order-wf0", WorkflowStatus.STARTED, MessageType.DEFAULT_VERSION),
                step("order-wf0", "reserveInventory", StepStatus.COMPLETED, MessageType.DEFAULT_VERSION));

        assertThatCode(() -> Invariants.assertVersionRoutingSound(log, "vorder-", HIGHEST, Set.of()))
                .as("non-versioned (order-) instances are out of INV-11's scope and are skipped")
                .doesNotThrowAnyException();
    }

    private static EventMessage step(String workflowId, String stepName, StepStatus status, String version) {
        Metadata metadata = MetadataUtils.create(workflowId, stepName, status);
        return new GenericEventMessage(new MessageType(stepName, version), Map.of(), metadata);
    }

    private static EventMessage workflowStatus(String workflowId, WorkflowStatus status, String version) {
        Metadata metadata = MetadataUtils.create(workflowId, status);
        return new GenericEventMessage(new MessageType("workflow", version), Map.of(), metadata);
    }
}
