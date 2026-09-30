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
