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
import io.axoniq.framework.workflow.simulation.scenarios.OneInstancePerStartScenario;
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
 * Exercises INVARIANTS.md INV-10 ({@code OneInstancePerStart}): a single workflow start for a given business key yields
 * exactly one live instance, and a duplicate/redelivered start while the instance is LIVE does not create a second
 * concurrent instance (the engine dedups it via its in-memory spawn-dedup repository).
 * <p>
 * The first test drives the real engine through {@code StartOnlyWorkflow} (one recorded step, then a never-arriving
 * {@code waitForEvent} that keeps it live), redelivers the start while live, and asserts the duplicate is deduped: still
 * exactly one workflow-status STARTED and one live instance. The remaining tests are assertion pins proving
 * {@link Invariants#assertOneInstancePerStart} is correct and not trivial: a single instance passes; a genuine
 * two-concurrent-live-instances history (two STARTED with no intervening terminal) throws; the documented after-terminal
 * re-spawn <strong>F-3</strong> (STARTED → terminal → STARTED) is tolerated (it is not an INV-10 break); and the
 * cross-instance interleaving of the global log (the F-2 surface, asserted per {@code workflowId}) is tolerated.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv10OneInstancePerStartTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void duplicateLiveStart_isDeduped_stillOneInstance() {
        OneInstancePerStartScenario.Outcome outcome = OneInstancePerStartScenario.run(0L, "A");

        assertThat(outcome.instanceLive())
                .as("StartOnlyWorkflow must reach a live (non-terminal) state so the live-dedup path is exercised")
                .isTrue();
        assertThat(outcome.startedCountAfterFirstStart())
                .as("a single start yields exactly one workflow-status STARTED")
                .isEqualTo(1);
        // INV-10 (the live-dedup property): redelivering the start while the instance is LIVE must NOT spawn a second
        // concurrent instance — still exactly one STARTED and one live execution for the id.
        assertThat(outcome.startedCountAfterDuplicate())
                .as("OneInstancePerStart: a duplicate live start is deduped — no second workflow-status STARTED")
                .isEqualTo(outcome.startedCountAfterFirstStart())
                .isEqualTo(1);
        assertThat(outcome.liveInstancesAfterDuplicate())
                .as("OneInstancePerStart: exactly one LIVE instance for the business key after the duplicate start")
                .isEqualTo(1);
    }

    @Test
    void assertOneInstancePerStart_passesForSingleInstance() {
        // One lifecycle: STARTED then a step. Exactly one live instance — must pass.
        List<EventMessage> log = List.of(
                workflowStatus("start-wf0", WorkflowStatus.STARTED),
                step("start-wf0", "reserveInventory", StepStatus.COMPLETED));

        assertThatCode(() -> Invariants.assertOneInstancePerStart(log))
                .as("a single started instance is not a concurrency break")
                .doesNotThrowAnyException();
    }

    @Test
    void assertOneInstancePerStart_detectsTwoConcurrentLiveInstances() {
        // The genuine break: the SAME id records a second workflow-status STARTED with NO intervening terminal status —
        // two concurrent live instances (a duplicate/redelivered start was not deduped while live). A real dedup
        // failure, distinct from F-3. Pins that the assertion catches it (so it is not trivially satisfied).
        List<EventMessage> log = List.of(
                workflowStatus("start-wf0", WorkflowStatus.STARTED),
                step("start-wf0", "reserveInventory", StepStatus.COMPLETED),
                workflowStatus("start-wf0", WorkflowStatus.STARTED)); // illegal: second live start, no terminal between

        assertThatThrownBy(() -> Invariants.assertOneInstancePerStart(log))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("OneInstancePerStart")
                .hasMessageContaining("start-wf0")
                .hasMessageContaining("two concurrent LIVE instances");
    }

    @Test
    void assertOneInstancePerStart_toleratesAfterTerminalRespawn_F3() {
        // FINDING F-3 (documented expected behaviour, NOT an INV-10 break): a start redelivered AFTER the instance
        // terminated re-spawns the key — STARTED → terminal → STARTED. INV-10 constrains concurrent/live duplicates
        // only; a STARTED that follows a terminal status is a fresh lifecycle of a finished key, tolerated here. See
        // formal/POC-TLA-DST.adoc (F-3) and Inv7TerminalIsFinalTest's F-3 probe.
        List<EventMessage> log = List.of(
                workflowStatus("start-wf0", WorkflowStatus.STARTED),
                step("start-wf0", "reserveInventory", StepStatus.COMPLETED),
                workflowStatus("start-wf0", WorkflowStatus.CANCELLED), // first lifecycle terminates
                workflowStatus("start-wf0", WorkflowStatus.STARTED),   // F-3: redelivered start re-spawns the key
                step("start-wf0", "reserveInventory", StepStatus.COMPLETED));

        assertThatCode(() -> Invariants.assertOneInstancePerStart(log))
                .as("the after-terminal re-spawn (F-3) is tolerated by INV-10 — it is not a concurrent-live break")
                .doesNotThrowAnyException();
    }

    @Test
    void assertOneInstancePerStart_toleratesCrossInstanceInterleaving() {
        // Two INDEPENDENT instances each start once; their workflow-status STARTED events interleave in the merged
        // global log. INV-10 is per-instance, so wf1 starting after wf0 is not "a second STARTED for wf0" — must pass.
        List<EventMessage> log = List.of(
                workflowStatus("start-wf0", WorkflowStatus.STARTED),
                workflowStatus("start-wf1", WorkflowStatus.STARTED),
                step("start-wf0", "reserveInventory", StepStatus.COMPLETED),
                step("start-wf1", "reserveInventory", StepStatus.COMPLETED));

        assertThatCode(() -> Invariants.assertOneInstancePerStart(log))
                .as("a different instance's STARTED is not a second live start for this instance (per-workflowId)")
                .doesNotThrowAnyException();
    }

    private static EventMessage step(String workflowId, String stepName, StepStatus status) {
        Metadata metadata = MetadataUtils.create(workflowId, stepName, status);
        return new GenericEventMessage(new MessageType(stepName), Map.of(), metadata);
    }

    private static EventMessage workflowStatus(String workflowId, WorkflowStatus status) {
        Metadata metadata = MetadataUtils.create(workflowId, status);
        return new GenericEventMessage(new MessageType("workflow"), Map.of(), metadata);
    }
}
