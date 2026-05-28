/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.dsl.api;

import io.axoniq.workflow.runtime.api.execution.context.*;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import jakarta.annotation.Nonnull;

import java.util.Map;

/**
 * Author-facing DSL API based on step definitions.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface WorkflowDSL {

    /**
     * Executes a step asynchronously from a DSL step definition.
     * <p>
     * This method is the bridge between the DSL-facing {@link ExecuteStepDefinition} and the runtime command-mode
     * primitive defined by ADR-001. The provided step definition is translated into the corresponding runtime command
     * and delegated to the execution layer.
     * </p>
     * <p>
     * The returned {@link WorkflowStepResult} is the durable handle for the started step. Callers may inspect the state
     * asynchronously, wait for completion explicitly, compose it with combinators, or pass it into typed resolution
     * methods built on top of the same handle.
     * </p>
     *
     * @param stepDefinition author-facing execute step definition containing step metadata, local payload, action,
     *                       payload mapping, timing, and retry configuration
     * @return durable asynchronous result handle for the execution step
     */
    @Nonnull
    WorkflowStepResult execute(@Nonnull ExecuteStepDefinition stepDefinition);

    /**
     * Executes a step synchronously from a DSL step definition and returns the resulting step payload.
     * <p>
     * This is the blocking counterpart of {@link #execute(ExecuteStepDefinition)}. It starts the step, waits until the
     * returned {@link WorkflowStepResult} completes, propagates any execution error, and then returns the resulting
     * payload map. If the step completes successfully without a result payload, an empty map is returned.
     * </p>
     *
     * @param stepDefinition author-facing execute step definition containing step metadata, local payload, action,
     *                       payload mapping, timing, and retry configuration
     * @return resulting payload map of the completed step, or an empty map when no result payload is produced
     * @throws RuntimeException any exception captured by the durable step result
     */
    @Nonnull
    Map<String, Object> awaitExecute(@Nonnull ExecuteStepDefinition stepDefinition);

    /**
     * Starts an asynchronous wait step from a DSL step definition.
     * <p>
     * This method translates the provided {@link WaitForStepDefinition} into the runtime wait command and delegates it
     * to the execution layer. The returned {@link WorkflowStepResult} completes when a matching event is received or
     * the configured timeout expires.
     * </p>
     *
     * @param stepDefinition author-facing wait step definition containing step metadata, event condition, payload
     *                       mapping, and timing
     * @return durable asynchronous result handle representing the active wait step
     */
    @Nonnull
    WorkflowStepResult waitForEvent(@Nonnull WaitForStepDefinition stepDefinition);

    /**
     * Starts a wait step from a DSL step definition and blocks until it completes.
     * <p>
     * This is the blocking counterpart of {@link #waitForEvent(WaitForStepDefinition)}. The method waits until the step
     * completes, propagates any failure from the underlying {@link WorkflowStepResult}, and returns the resulting
     * payload map. If the completed wait step has no result payload, an empty map is returned.
     * </p>
     *
     * @param stepDefinition author-facing wait step definition containing step metadata, event condition, payload
     *                       mapping, and timing
     * @return resulting payload map of the completed wait step, or an empty map when no result payload is produced
     * @throws RuntimeException any exception captured by the durable step result
     */
    @Nonnull
    Map<String, Object> awaitEvent(@Nonnull WaitForStepDefinition stepDefinition);

    /**
     * Applies a payload modification asynchronously from a DSL step definition.
     * <p>
     * This method bridges {@link PayloadStepDefinition} to the runtime payload modification primitive. The returned
     * {@link WorkflowStepResult} represents the durable completion handle for the modification step.
     * </p>
     *
     * @param stepDefinition author-facing payload step definition containing step metadata and payload modification
     * @return durable asynchronous result handle for the payload modification step
     */
    @Nonnull
    WorkflowStepResult modifyPayload(@Nonnull PayloadStepDefinition stepDefinition);

    /**
     * Applies a payload modification from a DSL step definition and blocks until it completes.
     * <p>
     * This is the blocking counterpart of {@link #modifyPayload(PayloadStepDefinition)}. The method waits for the
     * returned {@link WorkflowStepResult} to complete and propagates any failure from the underlying step.
     * </p>
     *
     * @param stepDefinition author-facing payload step definition containing step metadata and payload modification
     * @throws RuntimeException any exception captured by the durable step result
     */
    void awaitModifyPayload(@Nonnull PayloadStepDefinition stepDefinition);

    /**
     * Terminates the workflow with failure from a DSL step definition.
     * <p>
     * The provided {@link FailWorkflowDefinition} is translated into the runtime termination command and delegated to
     * the execution layer.
     * </p>
     *
     * @param definition author-facing failure definition containing step metadata and failure cause
     * @throws WorkflowFailedException always, after the failure event is published
     */
    void fail(@Nonnull FailWorkflowDefinition definition);

    /**
     * Terminates the workflow with cancellation from a DSL definition.
     * <p>
     * The provided {@link CancelWorkflowDefinition} is translated into the runtime termination command and delegated to
     * the execution layer.
     * </p>
     *
     * @param definition author-facing cancellation definition containing step metadata and optional cause
     * @throws WorkflowCancelledException always, after the cancellation event is published
     */
    void cancel(@Nonnull CancelWorkflowDefinition definition);

    /**
     * Terminates the step with cancellation from a DSL definition.
     * <p>
     * The provided {@link CancelStepDefinition} is translated into the runtime termination command and delegated to the
     * execution layer.
     * </p>
     *
     * @param definition author-facing cancellation definition containing step metadata and optional cause
     * @throws WorkflowCancelledException always, after the cancellation event is published
     */
    void cancelStep(@Nonnull CancelStepDefinition definition);
}
