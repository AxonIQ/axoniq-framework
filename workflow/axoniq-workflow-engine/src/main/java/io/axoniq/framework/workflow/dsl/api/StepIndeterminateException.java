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
package io.axoniq.framework.workflow.dsl.api;

import org.jspecify.annotations.Nullable;

/**
 * Exception used as the failure cause when a step's action was in-flight at a crash and, under the engine's
 * at-most-once execution guarantee, is <strong>not</strong> re-run on recovery.
 * <p>
 * A step's external side effect runs before its {@code COMPLETED} event commits. If the worker crashes in that window,
 * the durable {@code STARTED} record survives but the action's result does not, and the engine cannot tell whether the
 * side effect completed. Re-running it would make the effect run twice (at-least-once); to keep effects
 * <strong>at-most-once</strong> the engine refuses to re-run the attempt and instead resolves the step through the
 * regular error flow with this cause — its outcome is
 * <em>indeterminate</em> (the effect ran zero or one times). For a step without a retry policy this
 * surfaces as a {@code FAILED} step; with a retry policy it surfaces as a {@code RETRYING} attempt.
 * <p>
 * A subtype of {@link StepFailedException}: callers that want to handle any step-level failure can catch the parent;
 * callers that need to distinguish "in-flight at a crash, not re-run" specifically can catch this type. Distinct from
 * {@link StepTimedOutException} (the step exceeded its timeout) and {@link StepCancellationException} (explicit
 * cancellation): here the action was deliberately not re-executed to preserve the at-most-once guarantee.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class StepIndeterminateException extends StepFailedException {

    /**
     * Constructs a {@code StepIndeterminateException} for the given step.
     *
     * @param stepName the step whose in-flight attempt was interrupted by a crash and not re-run
     */
    public StepIndeterminateException(String stepName) {
        super("Step '" + stepName + "' was in-flight when the worker crashed; under the at-most-once "
                      + "execution guarantee it was not re-run on recovery, so its outcome is indeterminate "
                      + "(the side effect ran at most once).");
    }

    /**
     * Constructs a {@code StepIndeterminateException} with a custom message.
     *
     * @param message the detail message describing the indeterminate step
     * @param cause   the underlying cause, or {@code null} if none
     */
    public StepIndeterminateException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
