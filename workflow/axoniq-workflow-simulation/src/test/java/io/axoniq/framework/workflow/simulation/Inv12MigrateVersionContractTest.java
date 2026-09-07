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
import io.axoniq.framework.workflow.simulation.scenarios.MigrateVersionContractScenario;
import io.axoniq.framework.workflow.simulation.workflow.MigratingOrderWorkflow;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises INVARIANTS.md INV-12 ({@code MigrateVersionContract}): {@code ctx.migrateVersion(changeId, v)} obeys its
 * contract across crashes/replays — the recorded migration version for a {@code changeId} is set at most once and is
 * monotonic non-decreasing (first-writer-wins, never downgrades), and replaying the same history yields the same
 * recorded version (idempotent — replay does not re-apply or change it).
 * <p>
 * The first test drives the real engine through {@link MigratingOrderWorkflow}: a fresh start runs the body, which
 * migrates forward to {@link MigratingOrderWorkflow#MIGRATED_VERSION} via {@code ctx.migrateVersion}, recording the
 * marker exactly once and taking the migrated branch; a crash + replay re-reaches the call as a no-op so the recorded
 * version and marker count are unchanged. The remaining tests are assertion pins proving
 * {@link Invariants#assertMigrateVersionContract} is correct and not trivial: a single stable record passes; a history
 * recording the same {@code changeId} twice (replay re-applied the marker) throws; a history recording a downgraded
 * version for an instance throws; an invalid recorded version throws; the cross-instance interleaving of the global log
 * (the F-2 surface, asserted per {@code workflowId}) is tolerated; and non-migrating instances are skipped.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv12MigrateVersionContractTest {

    private static final String PREFIX = "vmig-";
    private static final String CHANGE = MigratingOrderWorkflow.CHANGE_ID; // payment-redesign
    private static final String INITIAL = MigratingOrderWorkflow.INITIAL_VERSION;  // 1.0.0
    private static final String MIGRATED = MigratingOrderWorkflow.MIGRATED_VERSION; // 1.0.1

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void migrateVersion_recordsOnce_takesMigratedBranch_stableAcrossReplay() {
        MigrateVersionContractScenario.Outcome outcome = MigrateVersionContractScenario.run(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("MigratingOrderWorkflow must reach a terminal (COMPLETED) workflow status")
                .isTrue();
        // Recorded at most once — and actually once (the body migrates), at the migrated version.
        assertThat(outcome.markerCountBeforeCrash())
                .as("MigrateVersionContract: the migration version is recorded exactly once")
                .isEqualTo(1L);
        assertThat(outcome.recordedVersionBeforeCrash())
                .as("MigrateVersionContract: the recorded version is the migrated-to version")
                .isEqualTo(MIGRATED);
        assertThat(outcome.tookMigratedBranch())
                .as("a fresh start migrates forward — the migrated branch (processV2) ran")
                .isTrue();
        assertThat(outcome.tookLegacyBranch())
                .as("a fresh start must NOT take the legacy branch — chargeV1 must be absent")
                .isFalse();
        // Stable across replay: a crash + replay re-reaches the migrateVersion call as a no-op (does not re-apply or
        // change the recorded version) — the marker count and recorded version are unchanged.
        assertThat(outcome.markerCountAfterCrash())
                .as("MigrateVersionContract: replay does not re-apply the marker — still recorded exactly once")
                .isEqualTo(outcome.markerCountBeforeCrash())
                .isEqualTo(1L);
        assertThat(outcome.recordedVersionAfterCrash())
                .as("MigrateVersionContract: replaying the same history resolves the same recorded version")
                .isEqualTo(outcome.recordedVersionBeforeCrash())
                .isEqualTo(MIGRATED);
    }

    @Test
    void assertMigrateVersionContract_passesForSingleStableRecord() {
        // One migrating instance: STARTED, a step, a single migration marker at the migrated version, COMPLETED.
        List<EventMessage> log = List.of(
                workflowStatus("vmig-wf0", WorkflowStatus.STARTED, INITIAL),
                step("vmig-wf0", MigratingOrderWorkflow.STEP_RESERVE_INVENTORY, StepStatus.COMPLETED, INITIAL),
                migration("vmig-wf0", CHANGE, MIGRATED),
                step("vmig-wf0", MigratingOrderWorkflow.STEP_PROCESS_V2, StepStatus.COMPLETED, MIGRATED),
                workflowStatus("vmig-wf0", WorkflowStatus.COMPLETED, MIGRATED));

        assertThatCode(() -> Invariants.assertMigrateVersionContract(log, PREFIX))
                .as("a single stable migration record (recorded once, valid version) is sound")
                .doesNotThrowAnyException();
    }

    @Test
    void assertMigrateVersionContract_detectsMarkerRecordedTwice() {
        // The genuine break: the SAME (workflowId, changeId) marker recorded TWICE — replay re-applied the migration
        // (or two writers recorded it). A real replay-stability failure. Pins that the assertion catches it.
        List<EventMessage> log = List.of(
                workflowStatus("vmig-wf0", WorkflowStatus.STARTED, INITIAL),
                migration("vmig-wf0", CHANGE, MIGRATED),
                // illegal: the marker for the same changeId was recorded a second time.
                migration("vmig-wf0", CHANGE, MIGRATED));

        assertThatThrownBy(() -> Invariants.assertMigrateVersionContract(log, PREFIX))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("MigrateVersionContract")
                .hasMessageContaining("vmig-wf0/" + CHANGE)
                .hasMessageContaining("at most once");
    }

    @Test
    void assertMigrateVersionContract_detectsRecordedDowngrade() {
        // A downgrade RECORDED for one instance: a later migration marker carries a version strictly LESS than an
        // earlier-recorded one. migrateVersion must reject a downgrade (IllegalArgumentException), never record it.
        List<EventMessage> log = List.of(
                workflowStatus("vmig-wf0", WorkflowStatus.STARTED, "1.0.0"),
                migration("vmig-wf0", "first-change", "1.2.0"),
                // illegal: a second change recorded a LOWER version than the first — a downgrade.
                migration("vmig-wf0", "second-change", "1.1.0"));

        assertThatThrownBy(() -> Invariants.assertMigrateVersionContract(log, PREFIX))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("MigrateVersionContract")
                .hasMessageContaining("vmig-wf0")
                .hasMessageContaining("monotonic non-decreasing");
    }

    @Test
    void assertMigrateVersionContract_detectsInvalidRecordedVersion() {
        // A migration marker carrying a non-semver recorded version — migrateVersion validates before recording, so a
        // malformed recorded version is a break.
        List<EventMessage> log = List.of(
                workflowStatus("vmig-wf0", WorkflowStatus.STARTED, INITIAL),
                migration("vmig-wf0", CHANGE, "not-a-version"));

        assertThatThrownBy(() -> Invariants.assertMigrateVersionContract(log, PREFIX))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("MigrateVersionContract")
                .hasMessageContaining("invalid recorded version");
    }

    @Test
    void assertMigrateVersionContract_toleratesCrossInstanceMarkers() {
        // Two INDEPENDENT migrating instances, each with their own single marker at the migrated version, interleaved in
        // the global log. INV-12 is per-instance, so wf1's marker is not "a second record for wf0" — must pass.
        List<EventMessage> log = List.of(
                workflowStatus("vmig-wf0", WorkflowStatus.STARTED, INITIAL),
                workflowStatus("vmig-wf1", WorkflowStatus.STARTED, INITIAL),
                migration("vmig-wf0", CHANGE, MIGRATED),
                migration("vmig-wf1", CHANGE, MIGRATED));

        assertThatCode(() -> Invariants.assertMigrateVersionContract(log, PREFIX))
                .as("independent instances each recording their own single marker are sound (per-workflowId)")
                .doesNotThrowAnyException();
    }

    @Test
    void assertMigrateVersionContract_skipsNonMigratingInstances() {
        // A different-prefix instance is not constrained by INV-12 — even a (hypothetical) double marker on it must be
        // skipped, not flagged (INV-12 is scoped to the migrating workflow's id prefix).
        List<EventMessage> log = List.of(
                workflowStatus("order-wf0", WorkflowStatus.STARTED, INITIAL),
                migration("order-wf0", CHANGE, MIGRATED),
                migration("order-wf0", CHANGE, MIGRATED));

        assertThatCode(() -> Invariants.assertMigrateVersionContract(log, PREFIX))
                .as("non-migrating (order-) instances are out of INV-12's scope and are skipped")
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

    private static EventMessage migration(String workflowId, String changeId, String version) {
        // A migration marker is a COMPLETED step event (stepName = changeId) carrying the versionChangeId + version
        // metadata keys — exactly what MetadataUtils.createVersionMigrationStep / EventMessageUtils.migrationStep emit.
        Metadata metadata = MetadataUtils.createVersionMigrationStep(workflowId, changeId, version);
        return new GenericEventMessage(new MessageType(changeId, version), Map.of("changeId", changeId, "version",
                                                                                  version), metadata);
    }
}
