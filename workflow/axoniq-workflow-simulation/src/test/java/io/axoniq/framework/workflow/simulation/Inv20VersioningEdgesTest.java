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
import io.axoniq.framework.workflow.simulation.scenarios.VersioningEdgesScenario;
import io.axoniq.framework.workflow.simulation.workflow.VersioningEdgesWorkflow;
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
 * Exercises INVARIANTS.md INV-20 ({@code VersioningEdges}): the versioning edges INV-11 ({@code VersionRoutingSound})
 * and INV-12 ({@code MigrateVersionContract}) do not cover — a {@code ctx.migrateVersion} downgrade is rejected (never
 * recorded), multiple distinct {@code changeId}s each record at most once and monotonic non-decreasing, and a deeper
 * 3-version registry routes a fresh start to the highest version and an instance recorded at an older version to the
 * closest registered sibling (never 0, never 2 definitions).
 * <p>
 * The first test drives the real engine through {@link VersioningEdgesScenario} ({@link VersioningEdgesWorkflow}
 * registered at three versions): a fresh start spawns at the highest version, migrates forward twice under distinct
 * {@code changeId}s, attempts a downgrade the engine rejects (never recorded — an observable {@code downgradeRejected}
 * step is recorded instead), then completes; a crash + replay re-reaches every migration as a no-op; and a second
 * instance recovered under a registry whose highest version was dropped routes to the closest registered sibling and
 * completes (the routing found a runnable body — never 0/never 2). The remaining tests are hand-built assertion pins
 * proving {@link Invariants#assertVersioningEdges} is correct and not trivial: a sound 3-version migrating history
 * passes; a recorded downgrade marker throws; the same {@code changeId} recorded twice throws; a recorded version below
 * the started version throws; a fresh spawn not at the highest registered version throws (with
 * {@code requireHighestForFresh}); a recovered instance under a reduced registry is tolerated (with
 * {@code requireHighestForFresh=false}); cross-instance interleaving (the F-2 surface, per {@code workflowId}) is
 * tolerated; and non-edges instances are skipped.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv20VersioningEdgesTest {

    private static final String PREFIX = "vedge-";
    private static final String HIGH = VersioningEdgesWorkflow.VERSION_HIGH;            // 2.0.0
    private static final String BUMP_1 = VersioningEdgesWorkflow.VERSION_BUMP_1;        // 2.1.0
    private static final String BUMP_2 = VersioningEdgesWorkflow.VERSION_BUMP_2;        // 2.2.0
    private static final String C1 = VersioningEdgesWorkflow.CHANGE_ID_1;               // ve-bump-1
    private static final String C2 = VersioningEdgesWorkflow.CHANGE_ID_2;               // ve-bump-2
    private static final String DOWNGRADE = VersioningEdgesWorkflow.CHANGE_ID_DOWNGRADE; // ve-downgrade

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void versioningEdges_freshSpawnHighest_downgradeRejected_multiChangeId_closestSiblingRouting() {
        VersioningEdgesScenario.Outcome outcome = VersioningEdgesScenario.run(0L);

        // Flow 1 — fresh spawn + downgrade-rejected + multi-changeId, stable across replay.
        assertThat(outcome.reachedTerminal())
                .as("VersioningEdgesWorkflow fresh-spawn instance must reach a terminal (COMPLETED) status")
                .isTrue();
        assertThat(outcome.spawnVersion())
                .as("a fresh start spawns at the highest registered version (2.0.0)")
                .isEqualTo(HIGH);
        assertThat(outcome.recordedMigrationVersions())
                .as("two distinct changeIds record strictly-increasing versions (2.1.0 then 2.2.0), monotonic")
                .containsExactly(BUMP_1, BUMP_2);
        assertThat(outcome.downgradeRecorded())
                .as("VersioningEdges: a downgrade migration must be REJECTED, never recorded — no marker for it")
                .isFalse();
        assertThat(outcome.downgradeRejectedStepRan())
                .as("the downgrade was genuinely attempted + rejected (the downgradeRejected step was recorded)")
                .isTrue();
        assertThat(outcome.markerCountAfterCrash())
                .as("VersioningEdges: replay re-reaches each migration as a no-op — marker count unchanged across crash")
                .isEqualTo(outcome.markerCountBeforeCrash())
                .isEqualTo(2L);

        // Flow 2 — deeper closest-sibling routing: the recovered instance found a runnable body and completed.
        assertThat(outcome.routedInstanceReachedTerminal())
                .as("VersioningEdges: an instance recovered under a registry missing its exact recorded version must "
                            + "route to the closest registered sibling (never 0 — stranded) and complete")
                .isTrue();
        assertThat(outcome.routedInstanceFinalized())
                .as("the routed instance ran its final step under the closest-sibling definition after recovery")
                .isTrue();
    }

    @Test
    void assertVersioningEdges_passesForSoundThreeVersionMigratingHistory() {
        // A fresh-spawn migrating instance: STARTED at the highest registered version (2.0.0), reserveInventory, two
        // forward migration markers (2.1.0 then 2.2.0) under distinct changeIds, the downgradeRejected step (no
        // downgrade marker), then post-migration steps + COMPLETED stamped at the bumped version. Sound.
        List<EventMessage> log = List.of(
                workflowStatus("vedge-A", WorkflowStatus.STARTED, HIGH),
                step("vedge-A", VersioningEdgesWorkflow.STEP_RESERVE_INVENTORY, StepStatus.COMPLETED, HIGH),
                migration("vedge-A", C1, BUMP_1),
                migration("vedge-A", C2, BUMP_2),
                step("vedge-A", VersioningEdgesWorkflow.STEP_DOWNGRADE_REJECTED, StepStatus.COMPLETED, BUMP_2),
                step("vedge-A", VersioningEdgesWorkflow.STEP_FINALIZE, StepStatus.COMPLETED, BUMP_2),
                workflowStatus("vedge-A", WorkflowStatus.COMPLETED, BUMP_2));

        assertThatCode(() -> Invariants.assertVersioningEdges(log, PREFIX, HIGH, DOWNGRADE, true, Set.of("vedge-A")))
                .as("a sound fresh-spawn 3-version migrating history (highest spawn, monotonic markers, no recorded "
                            + "downgrade) is sound")
                .doesNotThrowAnyException();
    }

    @Test
    void assertVersioningEdges_detectsRecordedDowngradeMarker() {
        // The genuine break: a migration marker for the DOWNGRADE changeId is committed — the engine wrongly recorded a
        // downgrade instead of rejecting it with IllegalArgumentException. A high-value versioning finding.
        List<EventMessage> log = List.of(
                workflowStatus("vedge-A", WorkflowStatus.STARTED, HIGH),
                migration("vedge-A", C1, BUMP_1),
                migration("vedge-A", C2, BUMP_2),
                // illegal: the downgrade migration was RECORDED (to 2.0.5, below the current 2.2.0).
                migration("vedge-A", DOWNGRADE, "2.0.5"));

        assertThatThrownBy(() -> Invariants.assertVersioningEdges(log, PREFIX, HIGH, DOWNGRADE, true, Set.of()))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("VersioningEdges")
                .hasMessageContaining("downgrade changeId")
                .hasMessageContaining(DOWNGRADE);
    }

    @Test
    void assertVersioningEdges_detectsMarkerRecordedTwiceForSameChangeId() {
        // Replay re-applied a marker (or two writers recorded it): the same (workflowId, changeId) recorded twice.
        List<EventMessage> log = List.of(
                workflowStatus("vedge-A", WorkflowStatus.STARTED, HIGH),
                migration("vedge-A", C1, BUMP_1),
                migration("vedge-A", C1, BUMP_1)); // illegal second marker for the same changeId.

        assertThatThrownBy(() -> Invariants.assertVersioningEdges(log, PREFIX, HIGH, DOWNGRADE, true, Set.of()))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("VersioningEdges")
                .hasMessageContaining("vedge-A/" + C1)
                .hasMessageContaining("at most once");
    }

    @Test
    void assertVersioningEdges_detectsRecordedVersionBelowStarted() {
        // A migration recorded a version BELOW the started version — a downgrade that should have been rejected.
        List<EventMessage> log = List.of(
                workflowStatus("vedge-A", WorkflowStatus.STARTED, HIGH),
                migration("vedge-A", C1, "1.0.0")); // illegal: 1.0.0 < started 2.0.0.

        assertThatThrownBy(() -> Invariants.assertVersioningEdges(log, PREFIX, HIGH, DOWNGRADE, true, Set.of()))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("VersioningEdges")
                .hasMessageContaining("below its started version");
    }

    @Test
    void assertVersioningEdges_detectsNonMonotonicRecordedVersions() {
        // Two distinct changeIds whose recorded versions move BACKWARDS (2.2.0 then 2.1.0) — a recorded downgrade across
        // changeIds.
        List<EventMessage> log = List.of(
                workflowStatus("vedge-A", WorkflowStatus.STARTED, HIGH),
                migration("vedge-A", C1, BUMP_2),
                migration("vedge-A", C2, BUMP_1)); // illegal: 2.1.0 after 2.2.0.

        assertThatThrownBy(() -> Invariants.assertVersioningEdges(log, PREFIX, HIGH, DOWNGRADE, true, Set.of()))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("VersioningEdges")
                .hasMessageContaining("monotonic non-decreasing");
    }

    @Test
    void assertVersioningEdges_detectsFreshSpawnNotAtHighest() {
        // A fresh spawn that did NOT pick the highest registered version (started at 1.5.0 when 2.0.0 is highest).
        List<EventMessage> log = List.of(
                workflowStatus("vedge-A", WorkflowStatus.STARTED, "1.5.0"),
                step("vedge-A", VersioningEdgesWorkflow.STEP_RESERVE_INVENTORY, StepStatus.COMPLETED, "1.5.0"));

        assertThatThrownBy(() -> Invariants.assertVersioningEdges(log, PREFIX, HIGH, DOWNGRADE, true, Set.of()))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("VersioningEdges")
                .hasMessageContaining("highest registered version");
    }

    @Test
    void assertVersioningEdges_toleratesRecoveredInstanceUnderReducedRegistry() {
        // The closest-sibling recovery facet: an instance spawned at the highest version (2.0.0) and migrated to 2.2.0,
        // recovered under a registry whose highest is now 1.5.0. With requireHighestForFresh=false the highest-for-fresh
        // check is NOT applied (the instance keeps its pinned recorded version; routing landing on a runnable body is
        // observed by completion in the scenario, not by a changed stamp) — the rest still holds, so it must pass.
        List<EventMessage> log = List.of(
                workflowStatus("vedge-B", WorkflowStatus.STARTED, HIGH),
                migration("vedge-B", C1, BUMP_1),
                migration("vedge-B", C2, BUMP_2),
                step("vedge-B", VersioningEdgesWorkflow.STEP_FINALIZE, StepStatus.COMPLETED, BUMP_2),
                workflowStatus("vedge-B", WorkflowStatus.COMPLETED, BUMP_2));

        assertThatCode(() -> Invariants.assertVersioningEdges(log, PREFIX, "1.5.0", DOWNGRADE, false, Set.of("vedge-B")))
                .as("a recovered instance under a reduced registry (requireHighestForFresh=false) is tolerated")
                .doesNotThrowAnyException();
    }

    @Test
    void assertVersioningEdges_toleratesCrossInstanceInterleaving() {
        // Two INDEPENDENT migrating instances, each with their own markers, interleaved in the global log. INV-20 is
        // per-instance, so one instance's markers are not "a second record" for the other — must pass.
        List<EventMessage> log = List.of(
                workflowStatus("vedge-A", WorkflowStatus.STARTED, HIGH),
                workflowStatus("vedge-B", WorkflowStatus.STARTED, HIGH),
                migration("vedge-A", C1, BUMP_1),
                migration("vedge-B", C1, BUMP_1),
                migration("vedge-A", C2, BUMP_2),
                migration("vedge-B", C2, BUMP_2));

        assertThatCode(() -> Invariants.assertVersioningEdges(log, PREFIX, HIGH, DOWNGRADE, true, Set.of()))
                .as("independent instances each recording their own markers are sound (per-workflowId)")
                .doesNotThrowAnyException();
    }

    @Test
    void assertVersioningEdges_skipsNonEdgesInstances() {
        // A different-prefix instance is not constrained by INV-20 — even a (hypothetical) recorded downgrade on it must
        // be skipped, not flagged (INV-20 is scoped to the versioning-edges workflow's id prefix).
        List<EventMessage> log = List.of(
                workflowStatus("order-wf0", WorkflowStatus.STARTED, HIGH),
                migration("order-wf0", DOWNGRADE, "1.0.0"));

        assertThatCode(() -> Invariants.assertVersioningEdges(log, PREFIX, HIGH, DOWNGRADE, true, Set.of()))
                .as("non-edges (order-) instances are out of INV-20's scope and are skipped")
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
