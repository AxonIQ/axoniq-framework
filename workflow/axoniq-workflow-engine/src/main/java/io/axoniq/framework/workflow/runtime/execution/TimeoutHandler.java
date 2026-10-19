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
 * Callback invoked by {@link ExecuteDelegate} when an execute attempt times out. The handler decides the step outcome
 * (e.g. publish TIMED_OUT or RETRYING).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
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
    void onTimeout(String stepName, EventNameCustomizer eventNameCustomizer);
}
