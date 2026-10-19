/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;

/**
 * Thrown when a {@link WorkflowEngine} cannot resolve a workflow configuration for a durable workflow definition it
 * encountered while restoring a segment.
 * <p>
 * A dedicated type distinguishes this specific, expected case from a genuine restore failure, so callers can log the
 * two differently.
 *
 * @author Steven van Beelen
 * @since 5.4.0
 */
@Internal
public class UnrecognizedWorkflowDefinitionException extends RuntimeException {

    /**
     * Constructs the exception with the given {@code message}.
     *
     * @param message detail message describing the unrecognized workflow definition
     */
    public UnrecognizedWorkflowDefinitionException(String message) {
        super(message);
    }
}
