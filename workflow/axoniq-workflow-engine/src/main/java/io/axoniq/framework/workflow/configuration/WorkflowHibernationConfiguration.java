/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import org.axonframework.common.AxonConfigurationException;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * The hibernation settings of the workflow engine, customized for the whole application through
 * {@link WorkflowConfigurer#hibernation}, or registered as a {@code WorkflowHibernationConfiguration} component, such
 * as the one Spring Boot creates from the {@code axon.workflow.hibernation} properties. A workflow module can override
 * them for its own workflows through
 * {@link WorkflowModule.OptionalPhase#hibernation(java.util.function.Function)}.
 * <p>
 * A hibernated workflow is removed from memory while it only waits for an external event or a deadline. It stays
 * live: the workflow manager finds it and can cancel it. It is restored from the event store when an event it may wait
 * for arrives, when its nearest deadline expires, or when a cancellation is requested for it, and replays its history
 * to the point where it waited. A workflow hibernates only when it made no progress for the {@link #idleDelay(Duration)
 * idle delay} and is at a safe point: its body waits on a step result, and every active step is an open
 * {@code waitForEvent} or {@code sleep}. A running {@code execute} step, also one waiting to retry, keeps it in memory.
 * <p>
 * Hibernation is a decision of each node: nothing is written when a workflow hibernates. A node that claims a segment
 * restores every running workflow of that segment, and those that keep waiting hibernate again after the idle delay.
 * <p>
 * Example usage:
 * <pre>{@code
 * WorkflowConfigurer.create()
 *                   .hibernation(hibernation -> hibernation.enabled(true)
 *                                                          .idleDelay(Duration.ofMinutes(1))
 *                                                          .wakeTimeout(Duration.ofSeconds(10)));
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public class WorkflowHibernationConfiguration {

    /**
     * The default {@link #idleDelay() idle delay}: 30 seconds.
     */
    public static final Duration DEFAULT_IDLE_DELAY = Duration.ofSeconds(30);
    /**
     * The default {@link #wakeTimeout() wake timeout}: 10 seconds.
     */
    public static final Duration DEFAULT_WAKE_TIMEOUT = Duration.ofSeconds(10);

    private boolean enabled = false;
    private Duration idleDelay = DEFAULT_IDLE_DELAY;
    private Duration wakeTimeout = DEFAULT_WAKE_TIMEOUT;

    /**
     * Enables or disables hibernation of waiting workflows. Defaults to {@code false}.
     *
     * @param enabled whether waiting workflows hibernate
     * @return this configuration, for a fluent API
     */
    public WorkflowHibernationConfiguration enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Sets the time a workflow must make no progress, meaning none of its own events is applied to it, before it
     * hibernates. Keeps workflows that wake often in memory, and a workflow restored by a wake or a claim stays in
     * memory for at least this long. A wake for a deadline or a cancellation that failed is tried again after the same
     * time. Defaults to {@link #DEFAULT_IDLE_DELAY 30 seconds}.
     *
     * @param idleDelay the time a workflow must make no progress before it hibernates
     * @return this configuration, for a fluent API
     * @throws AxonConfigurationException if {@code idleDelay} is not positive
     */
    public WorkflowHibernationConfiguration idleDelay(Duration idleDelay) {
        this.idleDelay = requirePositive(idleDelay, "idle delay");
        return this;
    }

    /**
     * Sets the time a wake may take to load a hibernated workflow from the event store. A wake by an event runs on the
     * thread of the event processor handling it, so this bounds how long it holds up that event processor. A wake that
     * takes longer fails. Defaults to {@link #DEFAULT_WAKE_TIMEOUT 10 seconds}.
     *
     * @param wakeTimeout the time a wake may take to load a workflow
     * @return this configuration, for a fluent API
     * @throws AxonConfigurationException if {@code wakeTimeout} is not positive
     */
    public WorkflowHibernationConfiguration wakeTimeout(Duration wakeTimeout) {
        this.wakeTimeout = requirePositive(wakeTimeout, "wake timeout");
        return this;
    }

    /**
     * Returns whether waiting workflows hibernate.
     *
     * @return {@code true} if waiting workflows hibernate
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * Returns the time a workflow must make no progress before it hibernates.
     *
     * @return the idle delay
     */
    public Duration idleDelay() {
        return idleDelay;
    }

    /**
     * Returns the time a wake may take to load a hibernated workflow.
     *
     * @return the wake timeout
     */
    public Duration wakeTimeout() {
        return wakeTimeout;
    }

    /**
     * Returns a copy of this configuration, which a workflow module customizes without changing the application's
     * settings.
     *
     * @return a copy of this configuration
     */
    WorkflowHibernationConfiguration copy() {
        return new WorkflowHibernationConfiguration().enabled(enabled)
                                                     .idleDelay(idleDelay)
                                                     .wakeTimeout(wakeTimeout);
    }

    private static Duration requirePositive(Duration duration, String name) {
        requireNonNull(duration, "The " + name + " must not be null.");
        if (duration.isNegative() || duration.isZero()) {
            throw new AxonConfigurationException("The " + name + " must be positive, but was " + duration + ".");
        }
        return duration;
    }
}
