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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.EventNameCustomizer;
import org.axonframework.common.annotation.Internal;

/**
 * Callback invoked by {@link ExecuteDelegate} when an execute attempt completes with a
 * non-timeout, non-cancellation error. The handler decides the step outcome
 * (e.g. publish FAILED or RETRYING).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@FunctionalInterface
@Internal
interface FailureHandler {

    /**
     * Handle a step execution error.
     *
     * @param stepName            the name of the failed step.
     * @param error               the error that caused the failure.
     * @param eventNameCustomizer event name customizer.
     */
    void onFailure(String stepName, Throwable error,
                   EventNameCustomizer eventNameCustomizer);
}
