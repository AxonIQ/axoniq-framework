/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation;

import org.axonframework.common.AxonConfigurationException;

/**
 * Thrown when the framework detects an event transformer chain misconfiguration.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
public final class ChainConfigurationException extends AxonConfigurationException {

    /**
     * Constructs a new exception with the given detail message.
     *
     * @param message human-readable description of the problem
     */
    public ChainConfigurationException(String message) {
        super(message);
    }
}
