/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import java.time.Duration;
import java.util.Objects;

/**
 * Lets a test outside this package run a hibernation pass of a {@link WorkflowEngine} now, instead of waiting for the
 * pass interval, and shorten the delay before a failed wake is tried again.
 *
 * @author Stefan Dragisic
 */
public final class WorkflowEngineHibernationPasses {

    private WorkflowEngineHibernationPasses() {
    }

    /**
     * Runs a hibernation pass of the given engine. Does nothing when hibernation is disabled.
     *
     * @param engine the engine whose workflows in memory are asked to hibernate
     */
    public static void runPass(WorkflowEngine engine) {
        var hibernation = engine.hibernation;
        if (hibernation != null) {
            hibernation.pass();
        }
    }

    /**
     * Sets the time after which the given engine wakes a hibernated workflow again when its wake failed.
     *
     * @param engine the engine whose wake retries to speed up
     * @param delay  the time between a failed wake and the next one
     */
    public static void retryFailedWakesAfter(WorkflowEngine engine, Duration delay) {
        hibernationOf(engine).wakeRetryDelay = delay;
    }

    private static WorkflowEngineHibernationSupport hibernationOf(WorkflowEngine engine) {
        return Objects.requireNonNull(engine.hibernation, "Hibernation is not enabled for this engine.");
    }
}
