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

import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.simulation.scenarios.RollingDeployScenario;
import io.axoniq.framework.workflow.simulation.workflow.RollingDeployWorkflow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase-3 production-realism pins: the registry changes across recoveries while instances are parked mid-flight (see
 * {@link RollingDeployScenario} for each ops story's mechanism). The recommended deploy and the
 * premature-removal-with-correct-authoring stories are healthy pins; the bad-deploy story pins the drift guard's
 * <strong>parked-instance blind spot</strong> (a structural change without {@code migrateVersion} executes silently
 * on recovery when the only post-insertion record is a non-terminal wait) and the <strong>poisoned rollback</strong>
 * it produces (the restored v1 body trips the guard on the bad deploy's terminal record — the operator's rollback
 * makes things worse, and only rolling forward releases the instance).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class RollingDeployTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void recommendedDeploy_parkedV1ResumesOnV1_freshSpawnsAtV2() {
        var outcome = RollingDeployScenario.recommendedDeploy(0L);

        // The pre-deploy instance resumed and completed on the RETAINED v1 body: no new step, no marker, pure-v1
        // history (the closest-sibling-≤ routing pass).
        assertThat(outcome.parkedTerminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(outcome.parkedFraudChecks()).as("the v1 instance never ran the v2-only step").isEqualTo(0);
        assertThat(outcome.parkedMarkers()).as("the v1 instance never migrated").isEqualTo(0);
        assertThat(outcome.parkedVersions())
                .as("every event of the v1 instance carries exactly the v1 version")
                .containsExactly(RollingDeployWorkflow.VERSION_V1);

        // The post-deploy instance spawned at the highest version and ran the gated new step. NOTE: no migration
        // marker — a fresh instance spawns AT the requested version, so migrateVersion's first-writer contract
        // (recorded >= requested) returns true WITHOUT recording a marker; markers appear only on genuine
        // lower-to-higher forks (the premature-removal pin).
        assertThat(outcome.freshTerminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(outcome.freshFraudChecks()).as("the fresh v2 instance ran the gated step").isEqualTo(1);
        assertThat(outcome.freshMarkers())
                .as("a fresh spawn at the requested version records no migration marker")
                .isEqualTo(0);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void prematureV1Removal_correctlyAuthoredV2_forksTheParkedInstanceMidFlight() {
        var outcome = RollingDeployScenario.prematureV1Removal(0L);

        // The intended ADR-005 rescue: the v1-recorded parked instance routes to v2 (closest-higher pass), forks
        // through the migrateVersion gate (marker recorded once), runs the new step, and completes.
        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(outcome.fraudChecks()).isEqualTo(1);
        assertThat(outcome.markers()).isEqualTo(1);
        assertThat(outcome.fulfils()).isEqualTo(1);
        // The mixed-version history is the DESIGNED fork shape: pre-fork events at v1, post-fork at v2, with the
        // marker recording the transition.
        assertThat(outcome.versions()).contains(RollingDeployWorkflow.VERSION_V1);
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void badDeploy_silentOnParkedInstance_poisonsTheRollback_rollForwardReleases() {
        var outcome = RollingDeployScenario.badDeployThenRollback(0L);

        // EXPECTED GAP (drift-guard blind spot): the structural change WITHOUT migrateVersion executed silently on
        // the recovered parked instance — no pause, no marker — because the guard's predicate fires only on
        // unreferenced TERMINAL steps and the parked instance's only post-insertion record was its non-terminal wait.
        // (Contrast INV-18: the same divergence past a terminal step pauses.)
        assertThat(outcome.fraudCheckRanSilently())
                .as("EXPECTED GAP: the un-gated new step ran silently on the recovered parked instance")
                .isTrue();
        assertThat(outcome.markersAfterBadDeploy()).as("nothing recorded the fork").isEqualTo(0);
        // Precise negative (refutes the version-flip hypothesis): the injected step's event inherits the INSTANCE's
        // recorded version (pinned from its STARTED event), not the new registration's — the history stays uniformly
        // v1, so the foreign step is durably indistinguishable from a v1-authored one. No version metadata betrays
        // the bad deploy.
        assertThat(outcome.versionsAfterBadDeploy())
                .as("the silent mutation is version-invisible: every event still carries the instance's v1")
                .containsExactly(RollingDeployWorkflow.VERSION_V1);

        // EXPECTED GAP (poisoned rollback): the restored v1 body re-parks cleanly, consumes the approval — and then
        // trips the drift guard on the bad deploy's fraudCheck COMPLETED at the fulfill step: paused non-terminally,
        // holding a consumed approval it cannot act on. The operator's rollback made things worse.
        assertThat(outcome.waitCompletedAfterRollback())
                .as("the approval WAS consumed before the pause")
                .isTrue();
        assertThat(outcome.terminalAfterRollback())
                .as("EXPECTED GAP: the rollback pauses the instance mid-completion (no terminal)")
                .isNull();
        assertThat(outcome.fulfilsAfterRollback()).as("fulfillment withheld by the pause").isEqualTo(0);

        // EXPECTED GAP (F-17, the drift pause's hidden cost): finishWorkflow runs the termination handler
        // unconditionally (the in-code TODO at SimpleWorkflowExecution.java:335 admits it), so the NON-terminal
        // drift-paused instance is removed from the repository as if finished — and, the repo now empty, the engine
        // persists the LATEST safe point, releasing the paused instance's recovery anchor.
        assertThat(outcome.liveAfterDriftPause())
                .as("EXPECTED GAP: the drift-paused instance is evicted from the live repository as if finished")
                .isFalse();

        // F-17, second half CLOSED: rehydration on claim restores every non-terminal instance from its own history,
        // independently of the safe point, so rolling forward to the matching body brings the abandoned instance back
        // (live again, drift-paused at the step the polluted history disagrees on). The eviction half above remains.
        assertThat(outcome.restoredByRollForward())
                .as("rolling forward to the matching body restores the abandoned instance")
                .isTrue();
        assertThat(outcome.terminalAfterRollForward()).isNull();
        assertThat(outcome.fraudChecksTotal()).isEqualTo(1);
    }
}
