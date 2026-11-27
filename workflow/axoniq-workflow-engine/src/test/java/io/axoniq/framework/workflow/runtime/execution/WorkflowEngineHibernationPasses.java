/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;

import java.util.Objects;

/**
 * Lets a test outside this package run a hibernation pass of a {@link WorkflowEngine} now, instead of waiting for the
 * hibernation interval.
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
     * Returns whether the given engine holds the given workflow hibernated.
     *
     * @param engine     the engine to inspect
     * @param workflowId the identifier of the workflow
     * @return {@code true} when the workflow is hibernated in the engine
     */
    public static boolean isHibernated(WorkflowEngine engine, String workflowId) {
        return hibernationOf(engine).isHibernated(workflowId);
    }

    /**
     * Runs the hibernation part of a segment release, which comes before the engine stops the segment's executions in
     * memory.
     *
     * @param engine  the engine that releases the segment
     * @param segment the segment being released
     */
    public static void startReleaseOf(WorkflowEngine engine, Segment segment) {
        hibernationOf(engine).release(segment);
    }

    private static WorkflowEngineHibernationSupport hibernationOf(WorkflowEngine engine) {
        return Objects.requireNonNull(engine.hibernation, "Hibernation is not enabled for this engine.");
    }
}
