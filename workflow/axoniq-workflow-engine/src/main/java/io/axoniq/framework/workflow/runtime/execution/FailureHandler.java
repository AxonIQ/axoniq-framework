/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import org.axonframework.common.annotation.Internal;

/**
 * Callback invoked by {@link ExecuteDelegate} when an execute attempt completes with a non-timeout, non-cancellation
 * error. The handler decides the step outcome (e.g. publish FAILED or RETRYING).
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
