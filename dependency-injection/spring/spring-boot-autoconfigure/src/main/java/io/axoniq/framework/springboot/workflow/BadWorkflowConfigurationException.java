/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot.workflow;

/**
 * Exception thrown when the workflow configuration is invalid.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class BadWorkflowConfigurationException extends RuntimeException {

    /**
     * Constructs new exception.
     *
     * @param message message.
     */
    BadWorkflowConfigurationException(String message) {
        super(message);
    }
}
