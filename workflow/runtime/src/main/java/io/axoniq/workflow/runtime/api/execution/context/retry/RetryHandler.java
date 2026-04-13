/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api.execution.context.retry;

/**
 * Callback invoked on each retry attempt, before publishing the RETRYING event.
 * Use for logging, metrics, or other side effects. Not called during replay.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@FunctionalInterface
public interface RetryHandler {

    RetryHandler NOOP = context -> {};

    void onRetry(RetryContext context);
}
