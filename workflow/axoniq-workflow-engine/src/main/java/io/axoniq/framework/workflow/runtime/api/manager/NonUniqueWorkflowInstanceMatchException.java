/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
