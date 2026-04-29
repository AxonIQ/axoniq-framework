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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

/**
 * Callback invoked by {@link ExecuteDelegate} when an execute attempt times out.
 * The handler decides the step outcome (e.g. publish TIMED_OUT or RETRYING).
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
@FunctionalInterface
interface TimeoutHandler {

    /**
     * Handle a step execution timeout.
     *
     * @param stepName            the name of the timed-out step.
     * @param eventNameCustomizer event name customizer.
     */
    void onTimeout(@Nonnull String stepName, @Nonnull EventNameCustomizer eventNameCustomizer);
}
