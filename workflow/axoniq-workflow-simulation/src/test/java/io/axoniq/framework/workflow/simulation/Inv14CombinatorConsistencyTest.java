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
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.CombinatorConsistencyScenario;
import io.axoniq.framework.workflow.simulation.workflow.CombinatorWorkflow;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises INVARIANTS.md INV-14 ({@code CombinatorConsistency}): a combinator's decision is consistent with the
 * documented short-circuit semantics, a pure function of its branch steps' committed terminal outcomes, and stable
 * across crash/replay.
 * <p>
 * The first test drives the real engine through {@link CombinatorWorkflow} (three parallel branches A=yes, B=no, C=no,
 * folded through {@code anyMatch}/{@code allMatch}/{@code noneMatch}): a fresh start resolves {@code anyMatch}=matched,
 * {@code allMatch}=unmatched, {@code noneMatch}=unmatched, records each decision, and a crash + replay rebuilds the
 * identical decisions. The remaining tests are assertion pins proving {@link Invariants#assertCombinatorConsistency} is
 * correct and not trivial: a consistent recorded decision passes; a recorded decision INCONSISTENT with the branch
 * outcomes (matched recorded while branches dictate unmatched) throws; recording both the matched and unmatched
 * post-combinator steps throws; the cross-instance interleaving of the global log (the F-2 surface, asserted per
 * {@code workflowId}) is tolerated; and an out-of-scope (non-combinator prefix) instance is skipped.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv14CombinatorConsistencyTest {

    private static final String PREFIX = "comb-";

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void combinatorDecisions_consistentWithBranchOutcomes_stableAcrossReplay() {
        CombinatorConsistencyScenario.Outcome outcome = CombinatorConsistencyScenario.run(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("CombinatorWorkflow must reach a terminal (COMPLETED) workflow status")
                .isTrue();
        // The fixed branch design (A=yes, B=no, C=no) under the VOTED_YES predicate fixes every decision.
        assertThat(outcome.decisionsBeforeCrash())
                .as("CombinatorConsistency: anyMatch matched, allMatch unmatched, noneMatch unmatched for A=yes,B=no,C=no")
                .containsEntry("anyMatch", true)
                .containsEntry("allMatch", false)
                .containsEntry("noneMatch", false);
        // anyMatch proceeds on the first (here only) matching branch — A.
        assertThat(outcome.anyMatchWinner())
                .as("the anyMatch race proceeds on the single matching branch A")
                .isEqualTo(CombinatorWorkflow.BRANCH_A);
        // Deterministic across replay: a crash + replay resolves the SAME decisions.
        assertThat(outcome.decisionsAfterCrash())
                .as("CombinatorConsistency: replaying the same history resolves the same combinator decisions")
                .isEqualTo(outcome.decisionsBeforeCrash());
    }

    @Test
    void assertCombinatorConsistency_passesForDecisionsConsistentWithBranchOutcomes() {
        // A=yes, B=no, C=no -> anyMatch matched, allMatch unmatched, noneMatch unmatched. The recorded post-combinator
        // steps and the reconstructed-payload decisions all agree with the semantics-derived expectation.
        List<EventMessage> log = consistentLog();
        Map<String, Map<String, Object>> actual = Map.of("comb-wf0", Map.of(
                CombinatorWorkflow.KEY_ANY_MATCHED, true,
                CombinatorWorkflow.KEY_ALL_MATCHED, false,
                CombinatorWorkflow.KEY_NONE_MATCHED, false,
                CombinatorWorkflow.KEY_ANY_WINNER, CombinatorWorkflow.BRANCH_A));

        assertThatCode(() -> Invariants.assertCombinatorConsistency(log, PREFIX, actual))
                .as("a combinator decision consistent with the committed branch outcomes is sound")
                .doesNotThrowAnyException();
    }

    @Test
    void assertCombinatorConsistency_detectsDecisionInconsistentWithBranchOutcomes() {
        // The genuine break: branch outcomes are A=yes,B=no,C=no, so allMatch MUST be unmatched — but the instance
        // recorded the allMatchMatched step (and an allMatched=true payload). A combinator resolved a decision its
        // branches' committed outcomes do not support. Pins that the assertion catches it.
        var log = new ArrayList<EventMessage>(branchLog());
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_ANY_MATCHED));
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_ALL_MATCHED));   // illegal: B/C voted no
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_NONE_UNMATCHED));
        Map<String, Map<String, Object>> actual = Map.of("comb-wf0", Map.of(
                CombinatorWorkflow.KEY_ANY_MATCHED, true,
                CombinatorWorkflow.KEY_ALL_MATCHED, true,
                CombinatorWorkflow.KEY_NONE_MATCHED, false,
                CombinatorWorkflow.KEY_ANY_WINNER, CombinatorWorkflow.BRANCH_A));

        assertThatThrownBy(() -> Invariants.assertCombinatorConsistency(log, PREFIX, actual))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("CombinatorConsistency")
                .hasMessageContaining("comb-wf0")
                .hasMessageContaining("allMatch");
    }

    @Test
    void assertCombinatorConsistency_detectsBothMatchedAndUnmatchedStepsRecorded() {
        // A combinator must resolve exactly one decision. Recording BOTH the matched and unmatched post-anyMatch steps
        // is contradictory and must throw.
        var log = new ArrayList<EventMessage>(branchLog());
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_ANY_MATCHED));
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_ANY_UNMATCHED)); // illegal: both recorded
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_ALL_UNMATCHED));
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_NONE_UNMATCHED));

        assertThatThrownBy(() -> Invariants.assertCombinatorConsistency(log, PREFIX, Map.of()))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("CombinatorConsistency")
                .hasMessageContaining("comb-wf0")
                .hasMessageContaining("BOTH");
    }

    @Test
    void assertCombinatorConsistency_toleratesCrossInstanceInterleaving() {
        // Two INDEPENDENT combinator instances, each soundly resolving its own decisions, interleaved in the global log.
        // INV-14 is per-instance, so wf1's branches/decisions are not "wf0's branches" — must pass.
        var log = new ArrayList<EventMessage>();
        log.addAll(branchLogFor("comb-wf0"));
        log.addAll(branchLogFor("comb-wf1"));
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_ANY_MATCHED));
        log.add(decisionStep("comb-wf1", CombinatorWorkflow.STEP_ANY_MATCHED));
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_ALL_UNMATCHED));
        log.add(decisionStep("comb-wf1", CombinatorWorkflow.STEP_ALL_UNMATCHED));
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_NONE_UNMATCHED));
        log.add(decisionStep("comb-wf1", CombinatorWorkflow.STEP_NONE_UNMATCHED));
        Map<String, Object> sound = Map.of(
                CombinatorWorkflow.KEY_ANY_MATCHED, true,
                CombinatorWorkflow.KEY_ALL_MATCHED, false,
                CombinatorWorkflow.KEY_NONE_MATCHED, false,
                CombinatorWorkflow.KEY_ANY_WINNER, CombinatorWorkflow.BRANCH_A);
        Map<String, Map<String, Object>> actual = Map.of("comb-wf0", sound, "comb-wf1", sound);

        assertThatCode(() -> Invariants.assertCombinatorConsistency(log, PREFIX, actual))
                .as("independent instances' combinator decisions are checked per-workflowId")
                .doesNotThrowAnyException();
    }

    @Test
    void assertCombinatorConsistency_skipsNonCombinatorInstances() {
        // A different-prefix instance is not constrained by INV-14 — even a (hypothetical) inconsistent combinator on it
        // must be skipped, not flagged (INV-14 is scoped to the combinator workflow's id prefix).
        var log = new ArrayList<EventMessage>(branchLogFor("order-wf0"));
        log.add(decisionStep("order-wf0", CombinatorWorkflow.STEP_ALL_MATCHED)); // inconsistent, but out of scope
        Map<String, Map<String, Object>> actual = Map.of("order-wf0",
                                                         Map.of(CombinatorWorkflow.KEY_ALL_MATCHED, true));

        assertThatCode(() -> Invariants.assertCombinatorConsistency(log, PREFIX, actual))
                .as("non-combinator (order-) instances are out of INV-14's scope and are skipped")
                .doesNotThrowAnyException();
    }

    @Test
    void assertCombinatorConsistency_skipsInstanceStillMidFlight() {
        // Only branch A has committed a terminal outcome (B/C still in flight). There is nothing to cross-check yet, so
        // the instance must be skipped (no false positive mid-run).
        List<EventMessage> log = List.of(branch("comb-wf0", CombinatorWorkflow.BRANCH_A, CombinatorWorkflow.VOTE_YES));

        assertThatCode(() -> Invariants.assertCombinatorConsistency(log, PREFIX, Map.of()))
                .as("an instance whose branches have not all committed a terminal outcome is skipped")
                .doesNotThrowAnyException();
    }

    /**
     * A consistent end-state log for {@code comb-wf0}: the three branches (A=yes, B=no, C=no) plus the three semantics-
     * correct post-combinator decision steps.
     */
    private static List<EventMessage> consistentLog() {
        var log = new ArrayList<EventMessage>(branchLog());
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_ANY_MATCHED));
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_ALL_UNMATCHED));
        log.add(decisionStep("comb-wf0", CombinatorWorkflow.STEP_NONE_UNMATCHED));
        return log;
    }

    /**
     * The three committed branch outcomes for {@code comb-wf0}: A votes yes, B and C vote no.
     */
    private static List<EventMessage> branchLog() {
        return branchLogFor("comb-wf0");
    }

    private static List<EventMessage> branchLogFor(String workflowId) {
        return List.of(
                branch(workflowId, CombinatorWorkflow.BRANCH_A, CombinatorWorkflow.VOTE_YES),
                branch(workflowId, CombinatorWorkflow.BRANCH_B, CombinatorWorkflow.VOTE_NO),
                branch(workflowId, CombinatorWorkflow.BRANCH_C, CombinatorWorkflow.VOTE_NO));
    }

    /**
     * A branch's committed COMPLETED step event carrying its {@code vote} result (combine reducer), exactly what the
     * engine emits for a {@code CombineGlobalAndLocalPayloadReducer} {@code execute} step.
     */
    private static EventMessage branch(String workflowId, String branchStep, String vote) {
        Metadata metadata = MetadataUtils.create(workflowId, branchStep, StepStatus.COMPLETED)
                                         .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD,
                                              CombineGlobalAndLocalPayloadReducer.NAME);
        return new GenericEventMessage(new MessageType(branchStep), Map.of(CombinatorWorkflow.KEY_VOTE, vote), metadata);
    }

    /**
     * A post-combinator decision COMPLETED step event (its distinct name encodes the recorded decision).
     */
    private static EventMessage decisionStep(String workflowId, String stepName) {
        Metadata metadata = MetadataUtils.create(workflowId, stepName, StepStatus.COMPLETED)
                                         .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD,
                                              CombineGlobalAndLocalPayloadReducer.NAME);
        return new GenericEventMessage(new MessageType(stepName), Map.of(), metadata);
    }
}
