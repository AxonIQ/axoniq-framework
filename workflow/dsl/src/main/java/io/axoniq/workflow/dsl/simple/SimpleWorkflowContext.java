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
package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.dsl.api.Payload;
import io.axoniq.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.ExecuteStepDefinition;
import io.axoniq.workflow.runtime.api.execution.context.WaitForStepDefinition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.state.StepTimedOutException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.association.Associations;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Convenience Java DSL built on top of {@link BaseWorkflowContext}.
 * <p>
 * This variant keeps the same workflow primitives as the base DSL, but adds shortcuts for common cases such as execute
 * steps without extra input payload, waiting for a typed event, and replacing the workflow payload with a single
 * object.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Allrad Buijze
 * @author Steven van Beelen
 * @since 1.0.0
 */
public class SimpleWorkflowContext extends BaseWorkflowContext {

    /**
     * Creates the simple workflow context for a single workflow instance.
     * <p>
     * Applications normally receive this context from the runtime when a workflow definition is executed rather than
     * constructing it directly.
     *
     * @param workflowId            unique identifier of the workflow instance
     * @param payload               initial workflow payload
     * @param processingContext     processing context for the current message
     * @param workflowConfiguration runtime configuration for this workflow
     */
    public SimpleWorkflowContext(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull ProcessingContext processingContext,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration
    ) {
        super(workflowId, payload, processingContext, workflowConfiguration);
    }

    /**
     * Starts an execute step with an empty step-local input payload.
     *
     * @param stepName  logical name of the step
     * @param processor function that performs the step work
     * @return handle for the running step
     */
    @Nonnull
    public WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nonnull PayloadProcessor processor
    ) {
        return execute(stepName, Map.of(), processor, UnaryOperator.identity());
    }

    /**
     * Starts an execute step with an empty step-local input payload and a customized step definition.
     *
     * @param stepName   logical name of the step
     * @param processor  function that performs the step work
     * @param customizer customizer for timeout, mapping, retries, or metadata
     * @return handle for the running step
     */
    @Nonnull
    public WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nonnull PayloadProcessor processor,
            @Nonnull UnaryOperator<ExecuteStepDefinition> customizer
    ) {
        return execute(stepName, Map.of(), processor, customizer);
    }

    /**
     * Runs a synchronous action as a workflow step and returns its single result value as the requested type.
     * <p>
     * This convenience method stores the action result under the internal key {@code "__" + stepName + "Result"} in the
     * step payload map and then unwraps that single value for the caller.
     *
     * @param stepName   logical name of the step
     * @param resultType expected Java type of the result
     * @param action     action to execute synchronously
     * @param <T>        expected result type
     * @return typed result returned by the action
     */
    @Nonnull
    public <T> T awaitExecute(
            @Nonnull String stepName,
            @Nonnull Class<T> resultType,
            @Nonnull Supplier<T> action
    ) {
        String resultKey = syntheticStepResultKey(stepName);
        return resultType.cast(awaitExecute(
                stepName,
                Map.of(),
                (pc, payload) -> Map.of(resultKey, action.get())
        ).get(resultKey));
    }

    @Nonnull
    private static String syntheticStepResultKey(@Nonnull String stepName) {
        return "__" + stepName + "Result";
    }

    /**
     * Blocks for the given duration without waiting for an external event.
     *
     * @param stepName logical name of the sleep step
     * @param timeout  time to wait before continuing
     */
    public void sleep(
            @Nonnull String stepName,
            @Nonnull Duration timeout
    ) {
        sleep(stepName, step -> step.timeout(timeout));
    }

    /**
     * Waits for an event of the given type that matches the provided association conditions and returns the converted
     * event payload.
     *
     * @param stepName   logical name of the waiting step
     * @param eventType  expected event payload type
     * @param conditions association constraints the event must satisfy
     * @param customizer customizer for timeout, mapping, or metadata
     * @param <T>        expected event payload type
     * @return converted event payload
     */
    @Nonnull
    public <T> T awaitEvent(
            @Nonnull String stepName,
            @Nonnull Class<T> eventType,
            @Nonnull Associations conditions,
            @Nonnull UnaryOperator<WaitForStepDefinition> customizer
    ) {
        var result = waitForEvent(stepName, eventType, conditions, customizer);
        result.await();
        if (result.success()) {
            return result.resultAs(eventType, processingContext().component(EventConverter.class))
                         .orElseThrow(() -> new IllegalStateException(
                                 "No event payload for step '" + stepName + "'"));
        }
        if (result.timeout()) {
            throw new StepTimedOutException(
                    "Step '" + stepName + "' timed out waiting for event " + eventType.getName());
        }
        if (result.canceled()) {
            throw new StepCancellationException(
                    "Step '" + stepName + "' was cancelled while waiting for event " + eventType.getName());
        }
        if (result.failure() && result.error().isPresent()) {
            throw result.error().orElseThrow();
        }
        throw new IllegalStateException("No event payload for step '" + stepName + "'");
    }

    /**
     * Waits for an event of the given type that matches the provided association conditions.
     *
     * @param stepName   logical name of the waiting step
     * @param eventType  expected event payload type
     * @param conditions association constraints the event must satisfy
     * @param customizer customizer for timeout, mapping, or metadata
     * @return handle for the waiting step
     */
    public WorkflowStepResult waitForEvent(
            @Nonnull String stepName,
            @Nonnull Class<?> eventType,
            @Nonnull Associations conditions,
            @Nonnull UnaryOperator<WaitForStepDefinition> customizer) {
        return waitForEvent(
                stepName,
                EventConditions.fromQualifiedName(resolve(eventType), conditions),
                customizer
        );
    }

    /**
     * Waits for an event of the given type that matches the provided association conditions.
     *
     * @param stepName   logical name of the waiting step
     * @param eventType  expected event payload type
     * @param conditions association constraints the event must satisfy
     * @return handle for the waiting step
     */
    public WorkflowStepResult waitForEvent(
            @Nonnull String stepName,
            @Nonnull Class<?> eventType,
            @Nonnull Associations conditions) {
        return super.waitForEvent(
                stepName,
                EventConditions.fromQualifiedName(resolve(eventType), conditions)
        );
    }

    /**
     * Replaces the current workflow payload with the fields extracted from the given value.
     *
     * @param stepName logical name of the payload update step
     * @param value    object whose properties become the new workflow payload
     */
    public void setPayload(
            @Nonnull String stepName,
            @Nonnull Object value
    ) {
        awaitModifyPayload(
                stepName,
                workflowPayload -> Payload.payload(workflowPayload)
                                          .with(Payload.payload(this, value))
                                          .getValues()
        );
    }

    /**
     * Cancels the workflow with a human-readable reason.
     *
     * @param reason descriptive reason for the cancellation
     * @throws WorkflowCancelledException always, after the cancellation event is published
     */
    public void cancel(String reason) {
        super.cancel(step -> step.cause(new WorkflowCancelledException(reason)));
    }
}
