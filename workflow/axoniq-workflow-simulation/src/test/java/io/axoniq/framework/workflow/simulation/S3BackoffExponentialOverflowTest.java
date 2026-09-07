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

import io.axoniq.framework.workflow.runtime.api.execution.context.retry.BackoffStrategy;
import io.axoniq.framework.workflow.simulation.scenarios.BackoffOverflowScenario;
import io.axoniq.framework.workflow.simulation.workflow.BackoffOverflowWorkflow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Settles finding <strong>F-9</strong> (S-3): {@code BackoffStrategy.exponential} overflowed at large attempt counts (an
 * INVARIANTS.md INV-8 {@code RetryBound} / INV-21 {@code RetryTimingAndExhaustionEdges} backoff-arithmetic edge), now
 * <strong>FIXED</strong>.
 * <p>
 * <strong>Was:</strong> {@code exponential(base, max)} did
 * {@code factor = 1L << (attempt - 1); computed = base.multipliedBy(factor); return computed > max ? max : computed;}
 * (runtime {@code BackoffStrategy.java:72-78}) — the cap was applied AFTER the multiply, so the multiply could throw
 * ({@code ArithmeticException} "Exceeds capacity of Duration" at the minute-base {@link BackoffOverflowWorkflow#OVERFLOW_ATTEMPT}th
 * attempt), go negative (attempt 64, {@code 1L << 63 == Long.MIN_VALUE}, uncapped) or wrap to a tiny delay (attempt 65,
 * shift masked mod 64) BEFORE the clamp. Computed on the workflow thread, the throw wedged the instance non-terminally
 * (the F-6/S-4 sink).
 * <p>
 * <strong>Now (fix):</strong> {@code exponential} clamps to {@code max} before any overflowing/negative/wrapped shift or
 * multiply, so {@code delay(attempt)} returns the capped {@code max} for every large attempt and never throws. Two layers:
 * <ol>
 *   <li><strong>Unit-level arithmetic pin</strong> on {@code BackoffStrategy.exponential(base, max).delay(attempt)}: small
 *       attempts behave normally and clamp at {@code max}; the formerly-overflowing minute-base attempt now returns the
 *       capped {@code max} (no throw); attempts 64/65 (the former negative/wrap corners) also clamp to {@code max}; and
 *       the delay never throws and is non-negative across a wide attempt range.</li>
 *   <li><strong>DST reachability</strong>: the {@link BackoffOverflowScenario} drives the real engine through an
 *       always-failing {@code execute} step under {@code maxRetries(70)} + the minute-scale exponential backoff. The step
 *       now retries on the clamped schedule to EXHAUSTION (sailing past the formerly-overflowing attempt), records
 *       terminal {@code FAILED}, and the workflow reaches a terminal status — no wedge, no hang. INV-8's bound is exact:
 *       {@code maxRetries} {@code RETRYING} records, {@code maxRetries + 1} attempt records.</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class S3BackoffExponentialOverflowTest {

    // ---- (1) unit-level arithmetic pin on BackoffStrategy.exponential(...).delay(attempt) ----

    @Test
    void exponential_smallAttempts_clampAtMax() {
        BackoffStrategy strategy = BackoffStrategy.exponential(Duration.ofMillis(10), Duration.ofSeconds(30));
        // attempt 1 -> 10ms * 2^0 = 10ms (below cap)
        assertThat(strategy.delay(1)).isEqualTo(Duration.ofMillis(10));
        // attempt 2 -> 10ms * 2^1 = 20ms (below cap)
        assertThat(strategy.delay(2)).isEqualTo(Duration.ofMillis(20));
        // attempt 30 -> 10ms * 2^29 ≈ 1491 hours, far above the cap -> clamped to 30s
        assertThat(strategy.delay(30)).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void exponential_clampsToMax_whereMultiplyWouldHaveOverflowedDurationCapacity() {
        // F-9 fix: a minute-scale base where 60s * 2^(attempt-1) would have exceeded Duration's capacity at OVERFLOW_ATTEMPT.
        // The fixed factory clamps to max BEFORE the (would-be-overflowing) multiply, so delay() returns max and never throws.
        BackoffStrategy strategy = BackoffStrategy.exponential(BackoffOverflowWorkflow.BACKOFF_BASE,
                                                              BackoffOverflowWorkflow.BACKOFF_MAX);
        assertThat(strategy.delay(BackoffOverflowWorkflow.OVERFLOW_ATTEMPT - 1))
                .as("the attempt just below the former overflow still computes the (capped) delay")
                .isEqualTo(BackoffOverflowWorkflow.BACKOFF_MAX);
        assertThatCode(() -> strategy.delay(BackoffOverflowWorkflow.OVERFLOW_ATTEMPT))
                .as("F-9: the formerly-overflowing attempt no longer throws")
                .doesNotThrowAnyException();
        assertThat(strategy.delay(BackoffOverflowWorkflow.OVERFLOW_ATTEMPT))
                .as("F-9: the formerly-overflowing attempt now clamps to max")
                .isEqualTo(BackoffOverflowWorkflow.BACKOFF_MAX);
    }

    @Test
    void exponential_attempt64and65_clampToMax_noNegativeOrWrappedGarbage() {
        // F-9 fix: at attempt 64 the OLD shift 1L << 63 was Long.MIN_VALUE (a negative, uncapped delay), and at attempt 65
        // the OLD shift masked mod 64 to a tiny wrapped delay. The fixed factory clamps the shift exponent and clamps to
        // max, so both former garbage corners now return the cap (a monotonic, non-negative schedule).
        BackoffStrategy strategy = BackoffStrategy.exponential(Duration.ofMillis(10), Duration.ofSeconds(30));
        assertThat(strategy.delay(64))
                .as("F-9: attempt 64 no longer goes negative — it clamps to max")
                .isEqualTo(Duration.ofSeconds(30));
        assertThat(strategy.delay(65))
                .as("F-9: attempt 65 no longer wraps to a tiny delay — it clamps to max")
                .isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void exponential_neverThrowsOrGoesNegative_acrossAWideAttemptRange() {
        // F-9 fix: the delay is non-negative and never throws for every attempt in 1..200 (covering the former overflow,
        // negative-shift, and wrap corners) for both a small-base and a minute-base configuration.
        BackoffStrategy small = BackoffStrategy.exponential(Duration.ofMillis(10), Duration.ofSeconds(30));
        BackoffStrategy minute = BackoffStrategy.exponential(BackoffOverflowWorkflow.BACKOFF_BASE,
                                                            BackoffOverflowWorkflow.BACKOFF_MAX);
        for (int attempt = 1; attempt <= 200; attempt++) {
            int a = attempt;
            assertThatCode(() -> {
                assertThat(small.delay(a)).as("small-base delay(" + a + ") non-negative").isGreaterThanOrEqualTo(Duration.ZERO);
                assertThat(minute.delay(a)).as("minute-base delay(" + a + ") non-negative").isGreaterThanOrEqualTo(Duration.ZERO);
            }).as("F-9: delay(" + a + ") never throws").doesNotThrowAnyException();
        }
    }

    // ---- (2) DST reachability: the fixed backoff lets the step exhaust and the workflow go terminal ----

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void exponentialBackoff_exhaustsAndGoesTerminal_findingF9Fixed() {
        BackoffOverflowScenario.Outcome outcome = BackoffOverflowScenario.run(0L, "A");

        assertThat(outcome.overflowAttemptReached())
                .as("the engine retried the failing step past the formerly-overflowing attempt "
                            + "(" + BackoffOverflowWorkflow.OVERFLOW_ATTEMPT + ") — the clamped delay() did not throw")
                .isTrue();
        // THE FIX (F-9): the clamped exponential backoff no longer throws, so the always-failing step exhausts its retries
        // and the workflow reaches a terminal status (the step goes FAILED). No non-terminal wedge.
        assertThat(outcome.reachedTerminal())
                .as("F-9 fixed: the clamped exponential backoff lets the instance reach a terminal workflow status "
                            + "(no non-terminal wedge)")
                .isTrue();
        assertThat(outcome.hasTerminalStep())
                .as("F-9 fixed: the failing step now records a terminal step status (FAILED) on exhaustion")
                .isTrue();
        // INV-8's record-level bound is exact: maxRetries RETRYING records + the single STARTED = maxRetries + 1 attempts.
        assertThat(outcome.retryingRecords())
                .as("the clamped schedule retries to exhaustion: one RETRYING per retry decision")
                .isEqualTo(BackoffOverflowWorkflow.MAX_RETRIES);
        assertThat(outcome.attemptRecords())
                .as("INV-8 holds exactly: attempt records (STARTED + RETRYING) == maxRetries + 1")
                .isEqualTo(BackoffOverflowWorkflow.MAX_RETRIES + 1);
    }
}
