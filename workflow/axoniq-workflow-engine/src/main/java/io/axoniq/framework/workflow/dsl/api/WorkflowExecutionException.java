/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;

/**
 * Stackless {@link RuntimeException} produced when a {@link WorkflowError} is rehydrated from an event payload.
 * <p>
 * Carries the fully-qualified class name of the originating throwable via {@link #type()} {@link #fillInStackTrace()}
 * is overridden to avoid capturing a stack trace at the reconstruction site
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
