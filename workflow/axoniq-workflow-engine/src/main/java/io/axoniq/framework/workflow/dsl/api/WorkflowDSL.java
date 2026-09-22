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
package io.axoniq.framework.workflow.dsl.api;

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.runtime.api.execution.context.CancelStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.CancelWorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.ExecuteStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.FailWorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.PayloadStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.PublishStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.VersionStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WaitForStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;

import java.util.Map;

/**
 * Author-facing DSL API based on step definitions.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
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
    WorkflowStepResult execute(ExecuteStepDefinition stepDefinition);

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
    Map<String, @Nullable Object> awaitExecute(ExecuteStepDefinition stepDefinition);

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
    WorkflowStepResult waitForEvent(WaitForStepDefinition stepDefinition);

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
    Map<String, @Nullable Object> awaitEvent(WaitForStepDefinition stepDefinition);

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
    WorkflowStepResult modifyPayload(PayloadStepDefinition stepDefinition);

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
    void awaitModifyPayload(PayloadStepDefinition stepDefinition);

    /**
     * Migrates the workflow to the version carried by {@code stepDefinition} for the given changeId
     * and returns whether the new branch is in effect for this workflow:
     * <ul>
     *   <li>Migration step already recorded → returns {@code true} iff the recorded version is
     *       {@code >=} the requested version.</li>
     *   <li>No step recorded yet, workflow already at the requested version → returns {@code true}
     *       without publishing a redundant step.</li>
     *   <li>No step recorded yet, requested version strictly greater than current → publishes the
     *       migration step and returns {@code true}; if the replay-drift guard fires (in-flight
     *       workflow already ran past this point), returns {@code false} and emits nothing.</li>
     *   <li>Downgrade attempt (requested {@code <} current, no step recorded) → throws
     *       {@link IllegalArgumentException}.</li>
     * </ul>
     *
     * @param stepDefinition author-facing version step definition.
     * @return {@code true} iff the workflow has committed to (or is past) the requested version for
     * this {@code changeId}; {@code false} if it stays on the legacy branch.
     */
    boolean migrateVersion(VersionStepDefinition stepDefinition);

    /**
     * Publishes a business event as a durable workflow step from a DSL step definition.
     * <p>
     * This method bridges {@link PublishStepDefinition} to the runtime publish primitive. Exactly one event is
     * appended: the definition's event enriched with workflow metadata, acting as both the business event and the
     * replay-safe checkpoint of the step. The returned {@link WorkflowStepResult} represents the durable completion
     * handle for the publish step.
     * </p>
     *
     * @param stepDefinition author-facing publish step definition containing step metadata and the event to publish
     * @return durable asynchronous result handle for the publish step
     */
    WorkflowStepResult publish(PublishStepDefinition stepDefinition);

    /**
     * Publishes a business event from a DSL step definition and blocks until the step is recorded.
     * <p>
     * This is the blocking counterpart of {@link #publish(PublishStepDefinition)}. The method waits for the returned
     * {@link WorkflowStepResult} to complete and propagates any failure from the underlying step.
     * </p>
     *
     * @param stepDefinition author-facing publish step definition containing step metadata and the event to publish
     * @throws RuntimeException any exception captured by the durable step result
     */
    void awaitPublish(PublishStepDefinition stepDefinition);

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
    void fail(FailWorkflowDefinition definition);

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
    void cancel(CancelWorkflowDefinition definition);

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
    void cancelStep(CancelStepDefinition definition);
}
