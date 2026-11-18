/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.util.WorkflowStateUtils;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Encapsulates the hibernation mechanics of a single {@link WorkflowExecution}: knowing whether it is at a safe point,
 * and leaving memory once the engine recorded it as hibernated.
 * <p>
 * A workflow is at a safe point when only the outside world can move it on: no step is in progress, meaning no
 * primitive call like {@code waitForEvent} is still running, no task is queued, no workflow cancellation is pending,
 * and every active step is a running, open wait for an event or a deadline. The check runs as a task, so at that
 * moment the body waits on a step result or a combinator.
 * <p>
 * An active {@code execute} step, also one waiting for a retry, has no wait condition, so it keeps the workflow in
 * memory. So does a wait whose timeout or completion is already being recorded, because it lost its condition, and a
 * workflow that is ending, because its terminal transition stops the running steps first.
 * <p>
 * The engine enables hibernation with {@link #enable(HibernationHandler)} and asks for it with {@link #request()}. The
 * request is a task on the execution's queue, so the workflow thread checks it the next time it waits. Once the
 * handler recorded the workflow as hibernated, the execution stops the way a released segment stops it, through
 * {@link WorkflowExecution#stopForShutdown()}, so it writes no events.
 * <p>
 * Internal: {@link SimpleWorkflowExecution} creates it and hands it to the {@link WorkflowEngineHibernationSupport}
 * through {@link WorkflowHibernationProvider}, the way {@link WorkflowExecutionCheckpointingSupport} holds the
 * checkpoint mechanics of an execution.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
final class WorkflowExecutionHibernationSupport {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutionHibernationSupport.class);

    private final WorkflowExecution execution;
    private final BooleanSupplier idle;
    private final EventWaitConditions eventWaitConditions;
    private final RunningSteps runningSteps;
    private volatile @Nullable HibernationHandler handler;
    private volatile boolean requested;

    /**
     * Creates the hibernation support of the given execution.
     *
     * @param execution           the execution this support belongs to
     * @param idle                whether no step is in progress, no task is queued and no workflow cancellation is
     *                            pending
     * @param eventWaitConditions the open wait conditions of the execution
     * @param runningSteps        the running steps of the execution
     */
    WorkflowExecutionHibernationSupport(WorkflowExecution execution,
                                        BooleanSupplier idle,
                                        EventWaitConditions eventWaitConditions,
                                        RunningSteps runningSteps) {
        this.execution = Objects.requireNonNull(execution, "The workflow execution must not be null.");
        this.idle = Objects.requireNonNull(idle, "The idle check must not be null.");
        this.eventWaitConditions = Objects.requireNonNull(eventWaitConditions,
                                                          "The event wait conditions must not be null.");
        this.runningSteps = Objects.requireNonNull(runningSteps, "The running steps must not be null.");
    }

    /**
     * Enables hibernation at safe points for the execution.
     *
     * @param handler records the execution as hibernated once it reaches a safe point
     */
    void enable(HibernationHandler handler) {
        this.handler = Objects.requireNonNull(handler, "The hibernation handler must not be null.");
    }

    /**
     * Asks the execution to hibernate at its next safe point.
     * <p>
     * The request is a task on the execution's task queue, so the workflow thread checks it the next time it waits. A
     * workflow that is not safe at that moment drops the request. Does nothing when hibernation is not enabled, the
     * execution is not running, or an earlier request is still queued.
     */
    void request() {
        if (handler == null || !execution.isRunning() || requested) {
            return;
        }
        requested = true;
        execution.appendTask(ignored -> hibernateIfSafe());
    }

    /**
     * Hands the execution to the handler, which hibernates it when it is at a safe point. Runs on the workflow thread,
     * as a task.
     */
    private void hibernateIfSafe() {
        requested = false;
        var current = handler;
        if (current == null) {
            return;
        }
        try {
            if (!current.hibernate(execution, this::openWaitsAtSafePoint)) {
                return;
            }
        } catch (RuntimeException e) {
            // When in doubt, a workflow is not safe to hibernate.
            logger.warn("Workflow {} stays in memory: hibernating it failed.", execution.workflowId(), e);
            return;
        }
        execution.stopForShutdown();
    }

    private Optional<Set<String>> openWaitsAtSafePoint() {
        var state = execution.state();
        var activeSteps = state.workflowStepNames()
                               .stream()
                               .filter(stepName -> WorkflowStateUtils.isStepActive(state, stepName))
                               .toList();
        var openWaits = eventWaitConditions.stepNames();
        var safe = idle.getAsBoolean()
                && !activeSteps.isEmpty()
                && openWaits.containsAll(activeSteps)
                && runningSteps.stepNames().containsAll(activeSteps);
        return safe ? Optional.of(openWaits) : Optional.empty();
    }

    /**
     * Records an execution as hibernated, on behalf of the engine that runs it.
     */
    @FunctionalInterface
    interface HibernationHandler {

        /**
         * Records the given execution as hibernated and removes it from memory, when it is at a safe point.
         * <p>
         * Called on the workflow thread. The handler checks the safe point itself, while it holds off incoming events
         * and cancellations, so none of them can reach the execution between the check and its removal.
         *
         * @param execution the execution asking to hibernate
         * @param safePoint the step names of the open waits when the execution is at a safe point, empty otherwise
         * @return {@code true} when the workflow is recorded as hibernated, {@code false} to keep it in memory
         */
        boolean hibernate(WorkflowExecution execution, Supplier<Optional<Set<String>>> safePoint);
    }
}
