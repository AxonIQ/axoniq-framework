/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.utils;

import io.axoniq.framework.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.dsl.api.StepCancellationException;
import io.axoniq.framework.workflow.dsl.api.PayloadProcessor;
import io.axoniq.framework.workflow.runtime.execution.ExecuteStepActionResolver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Resolver that lets tests provide execute-step actions.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class ManualExecuteStepActionResolver implements ExecuteStepActionResolver {

    private final ConcurrentHashMap<String, CompletableFuture<Function<ExecutePrimitive.ExecuteCommand, PayloadProcessor>>> actions =
            new ConcurrentHashMap<>();

    /**
     * Resolves the action for an execute step, blocking until the fixture supplies behavior for the step name.
     *
     * @param workflowExecutionOperations   workflow context
     * @param workflowExecution workflow execution
     * @param command           execute command
     * @return payload processor to execute
     */
    @Override
    public PayloadProcessor resolve(WorkflowExecutionOperations workflowExecutionOperations,
                                    WorkflowExecution workflowExecution,
                                    ExecutePrimitive.ExecuteCommand command) {
        var actionFactory = actions.computeIfAbsent(command.stepName(), ignored -> new CompletableFuture<>());
        try {
            return actionFactory.get().apply(command);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StepCancellationException("Interrupted while waiting for test action for step "
                                                        + command.stepName());
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve test action for step " + command.stepName(), e);
        }
    }

    /**
     * Applies the supplied action for the step instead of the workflow-defined action.
     *
     * @param stepName step name
     * @param action   action to apply
     */
    public void applyAction(String stepName, PayloadProcessor action) {
        complete(stepName, ignored -> action);
    }

    /**
     * Applies the workflow-defined action for the step.
     *
     * @param stepName step name
     */
    public void applyOriginalAction(String stepName) {
        complete(stepName, ExecutePrimitive.ExecuteCommand::action);
    }

    private void complete(String stepName,
                          Function<ExecutePrimitive.ExecuteCommand, PayloadProcessor> actionFactory) {
        actions.computeIfAbsent(stepName, ignored -> new CompletableFuture<>()).complete(actionFactory);
    }
}
