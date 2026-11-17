/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.event;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Keeps track of the {@link ScheduledExecutorService} instances created for persistent stream event sources, so they
 * can all be shut down together.
 * <p>
 * The {@link PersistentStreamConfigurationEnhancer} registers this registry as a component when none is present yet,
 * {@link #track(ScheduledExecutorService) tracks} every executor it hands to a persistent stream source, and invokes
 * {@link #shutdownAll()} from the component's shutdown handler. The shutdown of the executors thus follows the Axon
 * Framework shutdown lifecycle.
 *
 * @author Steven van Beelen
 * @see PersistentStreamConfigurationEnhancer
 * @since 5.5.0
 */
class PersistentStreamExecutorRegistry {

    private final List<ScheduledExecutorService> executors = new CopyOnWriteArrayList<>();

    /**
     * Starts tracking the given {@code executor}, so {@link #shutdownAll()} shuts it down.
     *
     * @param executor the executor to track
     * @return the given {@code executor}, to allow chaining
     */
    public ScheduledExecutorService track(ScheduledExecutorService executor) {
        executors.add(Objects.requireNonNull(executor, "executor must not be null"));
        return executor;
    }

    /**
     * Shuts down every {@link #track(ScheduledExecutorService) tracked} executor.
     */
    public void shutdownAll() {
        executors.forEach(ScheduledExecutorService::shutdown);
    }
}
