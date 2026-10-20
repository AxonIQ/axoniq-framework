/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.configuration;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the {@link MultiTenantStreamingProcessorRestarter}.
 * <p>
 * Register a customized instance to modify how the running streaming event processors are restarted when the set of
 * tenants changes. The {@link #DEFAULT} is registered automatically, so a customized instance is only needed to
 * deviate from it.
 *
 * @param restartTimeout the safety-net timeout bounding each processor's shutdown-and-start during a restart, so a
 *                       processor that never completes its shutdown or start cannot block the restart thread
 * @author Laura Devriendt
 * @since 5.3.0
 */
public record MultiTenantStreamingProcessorRestartConfiguration(Duration restartTimeout) {

    private static final Duration DEFAULT_RESTART_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Compact constructor validating that the given {@code restartTimeout} is a positive {@link Duration}.
     */
    @SuppressWarnings("MissingJavadoc")
    public MultiTenantStreamingProcessorRestartConfiguration {
        Objects.requireNonNull(restartTimeout, "The restart timeout must not be null.");
        if (!restartTimeout.isPositive()) {
            throw new IllegalArgumentException("The restart timeout must be positive.");
        }
    }

    /**
     * A default instance of the {@code MultiTenantStreamingProcessorRestartConfiguration}, setting the
     * {@link #restartTimeout()} to 30 seconds.
     */
    public static final MultiTenantStreamingProcessorRestartConfiguration DEFAULT =
            new MultiTenantStreamingProcessorRestartConfiguration(DEFAULT_RESTART_TIMEOUT);

    /**
     * Sets the safety-net timeout bounding each processor's shutdown-and-start during a restart. Raise it for a
     * deployment whose streaming event processors are slow to stop and start. Defaults to 30 seconds.
     *
     * @param restartTimeout the timeout bounding each processor's restart
     * @return a copy of this configuration using the given {@code restartTimeout}
     */
    public MultiTenantStreamingProcessorRestartConfiguration restartTimeout(Duration restartTimeout) {
        return new MultiTenantStreamingProcessorRestartConfiguration(restartTimeout);
    }
}
