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
package io.axoniq.workflow.runtime.api.execution.context.retry;

/**
 * Callback invoked on each retry attempt, before publishing the RETRYING event.
 * Use for logging, metrics, or other side effects. Not called during replay.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@FunctionalInterface
public interface RetryHandler {

    /**
     * Retry handler that performs no side effects.
     */
    RetryHandler NOOP = context -> {};

    /**
     * Invoked when a retry attempt is about to be published and executed.
     *
     * @param context retry context describing the step, attempt, and triggering failure
     */
    void onRetry(RetryContext context);
}
