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

import io.axoniq.framework.workflow.runtime.api.execution.state.StepTimedOutException;
import io.axoniq.framework.workflow.simulation.scenarios.BlockingAwaitTimeoutSurfaceScenario;
import io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow.CapturedSurface;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The former S-2 candidate <strong>F-8</strong> (blocking-convenience timeout-surfacing asymmetry) is <strong>fixed</strong>
 * on {@code main}: a step that TIMES OUT now surfaces {@code StepTimedOutException} on <em>every</em> blocking convenience
 * call, so the caller can always tell a timeout from a failure.
 * <p>
 * Originally only the typed {@code awaitEvent(stepName, Class, conditions, customizer)} overload surfaced
 * {@code StepTimedOutException}; the typed/untyped {@code awaitExecute} and untyped {@code awaitEvent} paths surfaced a
 * mis-classified {@code StepFailedException} with a {@code null} cause (a TIMED_OUT step's error field is {@code null}).
 * The fix special-cases {@code result.timeout()} → {@code StepTimedOutException} in
 * {@code AbstractDSLWorkflowContext.resolveStepPayload} (and the {@code SimpleWorkflowContext} helpers), symmetric across
 * all overloads — and {@code StepTimedOutException} now extends {@code StepFailedException}, so callers that only want
 * "any step-level failure" can still catch the parent.
 * <p>
 * This pin previously asserted the gap was <em>present</em> (the {@code F0EffectDuplicationTest} style); now it asserts
 * the consistent, fixed behaviour: all four blocking-convenience timeout paths surface {@code StepTimedOutException}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class F8BlockingAwaitTimeoutSurfaceTest {

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void blockingConvenienceTimeout_surfacesStepTimedOutException_consistently_F8Fixed() {
        BlockingAwaitTimeoutSurfaceScenario.Outcome outcome =
                BlockingAwaitTimeoutSurfaceScenario.run(0L, "A");

        // F-8 FIXED: every blocking-convenience timeout path surfaces StepTimedOutException — the asymmetry is gone.
        assertTimedOut(outcome.typedAwaitExecute(), "typed awaitExecute(stepName, Class, Supplier)");
        assertTimedOut(outcome.untypedAwaitExecute(), "untyped awaitExecute(stepName, Map, processor, customizer)");
        assertTimedOut(outcome.untypedAwaitEvent(), "untyped awaitEvent(stepName, EventCondition)");
        assertTimedOut(outcome.typedAwaitEvent(), "typed awaitEvent(stepName, Class, conditions, customizer)");
    }

    private static void assertTimedOut(CapturedSurface surface, String pathDescription) {
        assertThat(surface.thrownClassName())
                .as("F-8 fixed: %s must surface StepTimedOutException on timeout (no longer mis-classified as "
                            + "StepFailedException)", pathDescription)
                .isEqualTo(StepTimedOutException.class.getName());
    }
}
