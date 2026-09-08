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
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.invariants.Invariants.RetryStepSpec;
import io.axoniq.framework.workflow.simulation.scenarios.RetryTimingAndExhaustionEdgesScenario;
import io.axoniq.framework.workflow.simulation.workflow.RetryTimingWorkflow;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises INVARIANTS.md INV-21 ({@code RetryTimingAndExhaustionEdges}): the retry/timeout edges INV-8
 * ({@code RetryBound}) and INV-9 ({@code TimeoutsFire}) did not cover — backoff-strategy timing reconstructed from the
 * recorded {@code RETRYING} timestamps (survives crash/replay), a {@code retryWhile} predicate bounding the attempt
 * records, the {@code onRetry} handler firing once per actual retry decision and NOT re-firing on crash/replay (the F-0
 * analogue for retry handlers), and a per-attempt {@code execute} timeout reaching a terminal {@code TIMED_OUT}.
 * <p>
 * The first two tests drive the real engine through {@code RetryTimingWorkflow}: the retry-edges body (three backoff
 * strategies + a {@code retryWhile} predicate + the onRetry-not-re-fired probe across a crash + replay) and the
 * per-attempt-execute-timeout body (a slow {@code execute} blocking past its per-attempt timeout). The remaining tests
 * are assertion pins proving {@link Invariants#assertRetryTimingAndExhaustionEdges} and
 * {@link Invariants#assertOnRetryFiredOncePerRetry} are sound and non-trivial: a within-strategy history passes, an
 * over-bound / wrong-count / out-of-order / unresolved history throws, a shrinking linear/exponential gap throws, the
 * check is per {@code (workflowId, stepName)} and scoped to the id prefix, an onRetry re-fire (count > actual retries)
 * throws (the headline F-0-analogue), and a dropped onRetry fire (count < actual) throws.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv21RetryTimingAndExhaustionEdgesTest {

    private static final String PREFIX = "retryedge-";
    private static final String FIXED = RetryTimingWorkflow.STEP_FIXED;
    private static final String LINEAR = RetryTimingWorkflow.STEP_LINEAR;
    private static final Instant T0 = Instant.EPOCH.plusSeconds(100);

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void backoffStrategies_retryWhile_andOnRetryNotReFiredAcrossCrashReplay() {
        RetryTimingAndExhaustionEdgesScenario.RetryEdgesOutcome outcome =
                RetryTimingAndExhaustionEdgesScenario.runRetryEdges(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("RetryTimingWorkflow (retry edges) must reach a terminal workflow status")
                .isTrue();
        // Backoff strategy timing: each fixed/linear/exponential step fails twice then succeeds, so it records exactly
        // BACKOFF_MAX_RETRIES RETRYING records (and the schedule survived the virtual-time advancement / crash-replay,
        // which assertRetryTimingAndExhaustionEdges inside the scenario already verified content-based).
        assertThat(outcome.fixedRetrying())
                .as("fixed-backoff step records exactly BACKOFF_MAX_RETRIES retries")
                .isEqualTo(RetryTimingWorkflow.BACKOFF_MAX_RETRIES);
        assertThat(outcome.linearRetrying())
                .as("linear-backoff step records exactly BACKOFF_MAX_RETRIES retries")
                .isEqualTo(RetryTimingWorkflow.BACKOFF_MAX_RETRIES);
        assertThat(outcome.exponentialRetrying())
                .as("exponential-backoff step records exactly BACKOFF_MAX_RETRIES retries")
                .isEqualTo(RetryTimingWorkflow.BACKOFF_MAX_RETRIES);
        // retryWhile bound: the predicate stops retrying BEFORE maxRetries — strictly fewer attempts than maxRetries+1.
        assertThat(outcome.retryWhileRetrying())
                .as("retryWhile predicate stops after RETRY_WHILE_STOP_ATTEMPT-1 retries (fewer than maxRetries)")
                .isEqualTo(RetryTimingWorkflow.RETRY_WHILE_STOP_ATTEMPT - 1)
                .isLessThan(RetryTimingWorkflow.RETRY_WHILE_MAX_RETRIES);
        // onRetry fired exactly once per actual retry decision, for both a succeeding-after-retry step and the
        // retryWhile-bounded step.
        assertThat(outcome.fixedOnRetryBefore())
                .as("onRetry fired exactly once per committed RETRYING for the fixed step")
                .isEqualTo(outcome.fixedRetrying());
        assertThat(outcome.retryWhileOnRetry())
                .as("onRetry fired exactly once per committed RETRYING for the retryWhile step")
                .isEqualTo(outcome.retryWhileRetrying());
        // THE HEADLINE PROBE (F-0 analogue): a crash + replay must NOT re-invoke onRetry — its crash-surviving fire
        // count is unchanged. A grown count would be a real finding (onRetry re-ran on replay).
        assertThat(outcome.fixedOnRetryAfter())
                .as("RetryTimingAndExhaustionEdges (F-0 analogue): onRetry must NOT re-fire across a crash + replay — "
                            + "the fire count is unchanged from before the crash")
                .isEqualTo(outcome.fixedOnRetryBefore());
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void perAttemptExecuteTimeout_reachesTimedOut_andNeverHangs() {
        RetryTimingAndExhaustionEdgesScenario.TimeoutEdgeOutcome outcome =
                RetryTimingAndExhaustionEdgesScenario.runTimeoutEdge(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("RetryTimingWorkflow (timeout edge) must reach a terminal workflow status (never hangs)")
                .isTrue();
        // The whole point of the per-attempt execute timeout (the D5 residual INV-9 left to the wait path): the slow
        // execute, blocked past its per-attempt timeout, reaches a recorded TIMED_OUT outcome.
        assertThat(outcome.slowStepStatus())
                .as("RetryTimingAndExhaustionEdges: the slow execute step whose per-attempt timeout elapsed must be "
                            + "recorded TIMED_OUT")
                .isEqualTo(StepStatus.TIMED_OUT);
        // The attempt records stay within INV-8's bound (maxRetries + 1) on the timeout path too.
        assertThat(outcome.slowAttemptRecords())
                .as("attempt records on the timeout path stay within maxRetries+1")
                .isLessThanOrEqualTo(RetryTimingWorkflow.SLOW_EXECUTE_MAX_RETRIES + 1);
    }

    @Test
    void assertRetryTimingAndExhaustionEdges_passesForAWithinStrategyHistory() {
        // fixed maxRetries(2): STARTED + (RETRYING + RETRY_STARTED)×2 + COMPLETED — 2 retries, monotonic, terminal.
        List<EventMessage> log = List.of(
                step(PREFIX + "0", FIXED, StepStatus.STARTED, T0),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(200)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(250)),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(400)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(450)),
                step(PREFIX + "0", FIXED, StepStatus.COMPLETED, T0.plusMillis(500)));

        assertThatCode(() -> Invariants.assertRetryTimingAndExhaustionEdges(
                log, PREFIX, Map.of(FIXED, new RetryStepSpec(2, 2, false))))
                .as("a 2-retry fixed-backoff step that resolves COMPLETED satisfies INV-21")
                .doesNotThrowAnyException();
    }

    @Test
    void assertRetryTimingAndExhaustionEdges_throwsWhenRetriesExceedMaxRetries() {
        // The genuine break: maxRetries(2) but 3 RETRYING records.
        List<EventMessage> log = List.of(
                step(PREFIX + "0", FIXED, StepStatus.STARTED, T0),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(200)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(250)),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(400)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(450)),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(600)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(650)),
                step(PREFIX + "0", FIXED, StepStatus.FAILED, T0.plusMillis(700)));

        assertThatThrownBy(() -> Invariants.assertRetryTimingAndExhaustionEdges(
                log, PREFIX, Map.of(FIXED, new RetryStepSpec(2, 2, false))))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("RetryTimingAndExhaustionEdges")
                .hasMessageContaining(PREFIX + "0/" + FIXED);
    }

    @Test
    void assertRetryTimingAndExhaustionEdges_throwsWhenRetryCountIsNotTheStrategyDictatedCount() {
        // Within the bound (1 <= maxRetries 2) but the strategy dictates exactly 2 retries — a wrong count is the break.
        List<EventMessage> log = List.of(
                step(PREFIX + "0", FIXED, StepStatus.STARTED, T0),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(200)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(250)),
                step(PREFIX + "0", FIXED, StepStatus.COMPLETED, T0.plusMillis(300)));

        assertThatThrownBy(() -> Invariants.assertRetryTimingAndExhaustionEdges(
                log, PREFIX, Map.of(FIXED, new RetryStepSpec(2, 2, false))))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("RetryTimingAndExhaustionEdges")
                .hasMessageContaining("wrong number of retries");
    }

    @Test
    void assertRetryTimingAndExhaustionEdges_throwsWhenScheduleIsNotMonotonicInTime() {
        // The second RETRYING is committed BEFORE the first retry attempt started — a non-monotonic schedule.
        List<EventMessage> log = List.of(
                step(PREFIX + "0", FIXED, StepStatus.STARTED, T0),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(400)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(450)),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(200)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(480)),
                step(PREFIX + "0", FIXED, StepStatus.COMPLETED, T0.plusMillis(500)));

        assertThatThrownBy(() -> Invariants.assertRetryTimingAndExhaustionEdges(
                log, PREFIX, Map.of(FIXED, new RetryStepSpec(2, 2, false))))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("RetryTimingAndExhaustionEdges")
                .hasMessageContaining("not")
                .hasMessageContaining("monotonic");
    }

    @Test
    void assertRetryTimingAndExhaustionEdges_throwsWhenLinearOrExponentialGapShrinks() {
        // linear/exponential backoff gaps (RETRYING → next RETRY_STARTED) must be non-decreasing: the first backoff is
        // 1500ms, the second 100ms — a 1400ms shrink, unambiguously a GROSS schedule inversion (far beyond the
        // assertion's 250ms wall-clock jitter tolerance, so this pin stays decoupled from the exact tolerance value
        // while sub-ms real-log jitter never trips it).
        List<EventMessage> log = List.of(
                step(PREFIX + "0", LINEAR, StepStatus.STARTED, T0),
                step(PREFIX + "0", LINEAR, StepStatus.RETRYING, T0.plusMillis(100)),
                step(PREFIX + "0", LINEAR, StepStatus.RETRY_STARTED, T0.plusMillis(1600)),
                step(PREFIX + "0", LINEAR, StepStatus.RETRYING, T0.plusMillis(1700)),
                step(PREFIX + "0", LINEAR, StepStatus.RETRY_STARTED, T0.plusMillis(1800)),
                step(PREFIX + "0", LINEAR, StepStatus.COMPLETED, T0.plusMillis(1900)));

        assertThatThrownBy(() -> Invariants.assertRetryTimingAndExhaustionEdges(
                log, PREFIX, Map.of(LINEAR, new RetryStepSpec(2, 2, true))))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("RetryTimingAndExhaustionEdges")
                .hasMessageContaining("shrinking backoff gap");
    }

    @Test
    void assertRetryTimingAndExhaustionEdges_throwsWhenStepNeverResolves() {
        // STARTED + (RETRYING + RETRY_STARTED)×2 but NO terminal record — the retrying step hung.
        List<EventMessage> log = List.of(
                step(PREFIX + "0", FIXED, StepStatus.STARTED, T0),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(200)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(250)),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(400)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(450)));

        assertThatThrownBy(() -> Invariants.assertRetryTimingAndExhaustionEdges(
                log, PREFIX, Map.of(FIXED, new RetryStepSpec(2, 2, false))))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("RetryTimingAndExhaustionEdges")
                .hasMessageContaining("no terminal step");
    }

    @Test
    void assertRetryTimingAndExhaustionEdges_isPerWorkflowIdAndScopedToPrefix() {
        // Two INDEPENDENT in-prefix instances each at the bound (2 retries) must NOT pool; an out-of-prefix instance
        // with a wild retry count is ignored entirely.
        List<EventMessage> log = List.of(
                step(PREFIX + "0", FIXED, StepStatus.STARTED, T0),
                step(PREFIX + "1", FIXED, StepStatus.STARTED, T0),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(200)),
                step(PREFIX + "1", FIXED, StepStatus.RETRYING, T0.plusMillis(200)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(250)),
                step(PREFIX + "1", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(250)),
                step(PREFIX + "0", FIXED, StepStatus.RETRYING, T0.plusMillis(400)),
                step(PREFIX + "1", FIXED, StepStatus.RETRYING, T0.plusMillis(400)),
                step(PREFIX + "0", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(450)),
                step(PREFIX + "1", FIXED, StepStatus.RETRY_STARTED, T0.plusMillis(450)),
                step(PREFIX + "0", FIXED, StepStatus.COMPLETED, T0.plusMillis(500)),
                step(PREFIX + "1", FIXED, StepStatus.COMPLETED, T0.plusMillis(500)),
                // out-of-prefix instance — ignored even with absurd retry count.
                step("other-9", FIXED, StepStatus.STARTED, T0),
                step("other-9", FIXED, StepStatus.RETRYING, T0),
                step("other-9", FIXED, StepStatus.RETRYING, T0),
                step("other-9", FIXED, StepStatus.RETRYING, T0));

        assertThatCode(() -> Invariants.assertRetryTimingAndExhaustionEdges(
                log, PREFIX, Map.of(FIXED, new RetryStepSpec(2, 2, false))))
                .as("the check is per (workflowId, stepName) and scoped to the id prefix")
                .doesNotThrowAnyException();
    }

    @Test
    void assertOnRetryFiredOncePerRetry_passesWhenFiresEqualActualRetries() {
        assertThatCode(() -> Invariants.assertOnRetryFiredOncePerRetry(PREFIX + "0", FIXED, 2, 2))
                .as("onRetry fired exactly once per actual retry decision satisfies INV-21")
                .doesNotThrowAnyException();
    }

    @Test
    void assertOnRetryFiredOncePerRetry_throwsWhenOnRetryReFiresOnReplay() {
        // The headline F-0-analogue break: onRetry fired 3 times but only 2 RETRYING records — it re-ran on replay.
        assertThatThrownBy(() -> Invariants.assertOnRetryFiredOncePerRetry(PREFIX + "0", FIXED, 3, 2))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("RetryTimingAndExhaustionEdges")
                .hasMessageContaining("RE-FIRED on")
                .hasMessageContaining(PREFIX + "0/" + FIXED);
    }

    @Test
    void assertOnRetryFiredOncePerRetry_throwsWhenAFireIsDropped() {
        // The complementary break: onRetry fired fewer times than there were retries (a dropped handler fire).
        assertThatThrownBy(() -> Invariants.assertOnRetryFiredOncePerRetry(PREFIX + "0", FIXED, 1, 2))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("RetryTimingAndExhaustionEdges")
                .hasMessageContaining("fewer times");
    }

    private static EventMessage step(String workflowId, String stepName, StepStatus status, Instant timestamp) {
        Metadata metadata = MetadataUtils.create(workflowId, stepName, status);
        return new GenericEventMessage(UUID.randomUUID().toString(), new MessageType(stepName), Map.of(), metadata,
                                       timestamp);
    }
}
