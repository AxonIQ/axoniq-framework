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
package io.axoniq.framework.workflow.runtime.api.manager;

/**
 * Indicates that a query expected to identify one workflow instance matched more than one instance.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class NonUniqueWorkflowInstanceMatchException extends RuntimeException {

    /**
     * Creates an exception describing an ambiguous workflow instance query.
     *
     * @param message description of the ambiguous query result
     */
    public NonUniqueWorkflowInstanceMatchException(String message) {
        super(message);
    }
}
