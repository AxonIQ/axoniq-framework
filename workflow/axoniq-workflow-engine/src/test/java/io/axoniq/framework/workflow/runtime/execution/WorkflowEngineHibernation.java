/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Lets a test outside this package inspect the hibernation of a {@link WorkflowEngine}.
 *
 * @author Allard Buijze
 */
public final class WorkflowEngineHibernation {

    private WorkflowEngineHibernation() {
    }

    /**
     * Returns whether hibernation is enabled for the given engine.
     *
     * @param engine the engine to inspect
     * @return {@code true} when the engine may hibernate workflows
     */
    public static boolean isEnabled(WorkflowEngine engine) {
        return engine.isHibernationEnabled();
    }

    /**
     * Returns whether the given engine holds the given workflow hibernated.
     *
     * @param engine     the engine to inspect
     * @param workflowId the identifier of the workflow
     * @return {@code true} when the workflow is hibernated in the engine
     */
    public static boolean isHibernated(WorkflowEngine engine, String workflowId) {
        return engine.hibernatedWorkflowIds().contains(workflowId);
    }

    /**
     * Returns whether the current claim of the given segment in the given engine caught up with the startup head, so
     * the bodies of its workflows may run.
     *
     * @param engine  the engine to inspect
     * @param segment the segment whose claim to inspect
     * @return {@code true} when the engine holds a current claim of the segment that caught up
     */
    public static boolean isCaughtUp(WorkflowEngine engine, Segment segment) {
        return engine.segmentClaims.claimOf(segment)
                                   .map(claim -> !claim.isWaiting())
                                   .orElse(false);
    }

    /**
     * Starts a new claim of the given segment in the given engine, on the execution context of its current claim,
     * without stopping the executions of that claim or restoring anything: the first step of a claim, which ends the
     * current claim, before the claim stops that claim's executions.
     *
     * @param engine  the engine whose segment is claimed again
     * @param segment the segment the engine currently holds a claim of
     */
    public static void startNewClaimOf(WorkflowEngine engine, Segment segment) {
        var current = engine.segmentClaims.claimOf(segment).orElseThrow();
        engine.segmentClaims.startClaim(segment, Objects.requireNonNull(current.executionContext()));
    }

    /**
     * Asks the execution of the given workflow in the given engine to hibernate at its next safe point, also when it
     * made progress within the idle delay, the way a hibernation pass asks an execution that made none for that long.
     * Does nothing when the workflow is not in memory.
     *
     * @param engine     the engine running the workflow
     * @param workflowId the identifier of the workflow
     */
    public static void requestHibernation(WorkflowEngine engine, String workflowId) {
        engine.workflowExecutions()
              .stream()
              .filter(execution -> execution.workflowId().equals(workflowId))
              .filter(WorkflowHibernationProvider.class::isInstance)
              .forEach(execution -> ((WorkflowHibernationProvider) execution).workflowHibernation()
                                                                            .request(Duration.ZERO));
    }

    /**
     * Returns how often the execution of the given workflow in the given engine checked whether it may hibernate, as
     * it does for every request of a hibernation pass once it made no progress for the idle delay, and for every
     * {@link #requestHibernation(WorkflowEngine, String) explicit request}. Each check is a moment at which a workflow
     * that may hibernate leaves memory, so a workflow still in memory after a check stayed there for a reason.
     *
     * @param engine     the engine running the workflow
     * @param workflowId the identifier of the workflow
     * @return the number of checks its execution completed, or empty when the workflow is not in memory
     */
    public static OptionalInt completedHibernationChecks(WorkflowEngine engine, String workflowId) {
        return engine.workflowExecutions()
                     .stream()
                     .filter(execution -> execution.workflowId().equals(workflowId))
                     .filter(WorkflowHibernationProvider.class::isInstance)
                     .mapToInt(execution -> ((WorkflowHibernationProvider) execution).workflowHibernation()
                                                                                    .completedChecks
                                                                                    .get())
                     .findFirst();
    }

    /**
     * Returns whether the execution of the given workflow in the given engine was asked to check whether it may
     * hibernate, and did not complete that check yet. A hibernation pass asks for such a check only once the workflow
     * made no progress for the idle delay, and the execution runs it later, as one of its tasks.
     *
     * @param engine     the engine running the workflow
     * @param workflowId the identifier of the workflow
     * @return {@code true} when the workflow is in memory and a check it was asked for is still to complete
     */
    public static boolean hasPendingHibernationCheck(WorkflowEngine engine, String workflowId) {
        return engine.workflowExecutions()
                     .stream()
                     .filter(execution -> execution.workflowId().equals(workflowId))
                     .filter(WorkflowHibernationProvider.class::isInstance)
                     .map(execution -> ((WorkflowHibernationProvider) execution).workflowHibernation())
                     .anyMatch(hibernation -> hibernation.requestedChecks.get() > hibernation.completedChecks.get());
    }

    /**
     * Requests a checkpoint of the given segment in the given engine at the given token, as the engine does when an
     * event was handled or a workflow finished.
     *
     * @param engine  the engine holding the segment
     * @param segment the segment to checkpoint
     * @param token   the token to checkpoint at, where {@link TrackingToken#LATEST} stands for the last token the
     *                processor handed to the segment
     */
    public static void requestCheckpoint(WorkflowEngine engine, Segment segment, TrackingToken token) {
        engine.checkpointingSupport().requestCheckpoint(segment, token);
    }
}
