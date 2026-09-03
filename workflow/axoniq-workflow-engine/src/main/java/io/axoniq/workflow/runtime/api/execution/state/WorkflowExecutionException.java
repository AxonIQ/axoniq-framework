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

import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;

/**
 * Stackless {@link RuntimeException} produced when a {@link WorkflowError} is rehydrated from an event payload.
 * <p>
 * Carries the fully-qualified class name of the originating throwable via {@link #type()}
 * {@link #fillInStackTrace()} is overridden to avoid capturing a stack trace at the reconstruction site
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public final class WorkflowExecutionException extends RuntimeException implements Cause {

    private final String type;

    /**
     * Creates a stackless exception carrying the originating throwable's metadata.
     *
     * @param type    fully-qualified class name of the originating throwable.
     * @param message message of the originating throwable, or {@code null}.
     * @param cause   reconstructed cause, or {@code null}.
     */
    public WorkflowExecutionException(String type,
                                      @Nullable String message,
                                      @Nullable Throwable cause) {
        super(message, cause);
        this.type = type;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    @Nullable
    public String message() {
        return getMessage();
    }

    @Override
    public String toString() {
        return getMessage() != null ? type + ": " + getMessage() : type;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
