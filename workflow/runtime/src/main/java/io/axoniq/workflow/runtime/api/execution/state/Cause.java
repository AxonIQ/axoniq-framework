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
package io.axoniq.workflow.runtime.api.execution.state;

import org.axonframework.common.annotation.Internal;

/**
 * Contract describing the cause of a workflow or step failure.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
public interface Cause {

    /**
     * Fully-qualified class name of the originating {@link Throwable}.
     */
    String type();

    /**
     * Message describing the cause, typically {@link Throwable#getMessage()}.
     */
    String message();

    /**
     * Returns {@code true} when {@link #type()} matches the fully-qualified name of {@code clazz}.
     * Use this instead of {@code instanceof} when branching on the originating throwable's class —
     * {@code instanceof} does not match because the runtime cause is always
     * {@link WorkflowExecutionException}, regardless of the originating type.
     */
    default boolean isType(Class<? extends Throwable> clazz) {
        return clazz.getName().equals(type());
    }
}
