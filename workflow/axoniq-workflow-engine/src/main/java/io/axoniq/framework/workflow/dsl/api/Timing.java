/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;


import java.time.Duration;

/**
 * Reusable timing configuration shared by primitive specs.
 *
 * @param timeout timeout for primitive completion
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public record Timing(Duration timeout) {

    /**
     * Returns a copy of this timing specification with the provided timeout.
     *
     * @param timeout timeout for primitive completion
     * @return copied timing specification with updated timeout
     */
    public Timing timeout(Duration timeout) {
        return new Timing(timeout);
    }

    /**
     * Returns a copy of this timing specification with the provided timeout in seconds.
     *
     * @param timeoutSeconds timeout in seconds
     * @return copied timing specification with updated timeout
     */
    public Timing timeout(long timeoutSeconds) {
        return timeout(Duration.ofSeconds(timeoutSeconds));
    }
}
