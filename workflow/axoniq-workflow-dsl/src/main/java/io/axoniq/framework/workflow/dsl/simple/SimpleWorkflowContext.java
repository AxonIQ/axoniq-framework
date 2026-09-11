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
package io.axoniq.framework.workflow.dsl.simple;

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.dsl.api.Payload;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.context.ExecuteStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WaitForStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepTimedOutException;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.framework.workflow.runtime.association.Associations;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Convenience Java DSL built on top of {@link BaseWorkflowContext}.
 * <p>
 * This variant keeps the same workflow primitives as the base DSL, but adds shortcuts for common cases such as execute
 * steps without extra input payload, waiting for a typed event, publishing an event given as payload, and
 * replacing the workflow payload with a single object.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Allrad Buijze
 * @author Steven van Beelen
 * @since 5.4.0
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
            String workflowId,
            Map<String, @Nullable Object> payload,
            ProcessingContext processingContext,
            WorkflowConfiguration<?> workflowConfiguration
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
    public WorkflowStepResult execute(
            String stepName,
            PayloadProcessor processor
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
    public WorkflowStepResult execute(
            String stepName,
            PayloadProcessor processor,
            UnaryOperator<ExecuteStepDefinition> customizer
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
    public <T> T awaitExecute(
            String stepName,
            Class<T> resultType,
            Supplier<T> action
    ) {
        String resultKey = syntheticStepResultKey(stepName);
        return resultType.cast(awaitExecute(
                stepName,
                Map.of(),
                (pc, payload) -> Map.of(resultKey, action.get())
        ).get(resultKey));
    }

    private static String syntheticStepResultKey(String stepName) {
        return "__" + stepName + "Result";
    }

    /**
     * Blocks for the given duration without waiting for an external event.
     *
     * @param stepName logical name of the sleep step
     * @param timeout  time to wait before continuing
     */
    public void sleep(
            String stepName,
            Duration timeout
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
    public <T> T awaitEvent(
            String stepName,
            Class<T> eventType,
            Associations conditions,
            UnaryOperator<WaitForStepDefinition> customizer
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
            String stepName,
            Class<?> eventType,
            Associations conditions,
            UnaryOperator<WaitForStepDefinition> customizer) {
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
            String stepName,
            Class<?> eventType,
            Associations conditions) {
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
            String stepName,
            Object value
    ) {
        awaitModifyPayload(
                stepName,
                workflowPayload -> Payload.payload(workflowPayload)
                                          .with(Payload.payload(this, value))
                                          .getValues()
        );
    }

    /**
     * Publishes a business event as a durable workflow step and returns a handle to await later.
     *
     * @param stepName logical name of the publish step
     * @param event    event to publish
     * @return handle for the publish step
     * @see BaseWorkflowContext#publish(String, EventMessage, UnaryOperator)
     */
    public WorkflowStepResult publish(String stepName, EventMessage event) {
        return publish(stepName, event, UnaryOperator.identity());
    }

    /**
     * Publishes a business event given as payload. The {@link org.axonframework.messaging.core.MessageType} is resolved
     * through the configured {@link MessageTypeResolver}, as {@code EventAppender#append(Object)} does.
     *
     * @param stepName logical name of the publish step
     * @param payload  event payload
     * @return handle for the publish step
     */
    public WorkflowStepResult publish(String stepName, Object payload) {
        return publish(stepName, payload, Metadata.emptyInstance());
    }

    /**
     * Publishes a business event given as payload and metadata. The {@link org.axonframework.messaging.core.MessageType}
     * is resolved through the configured {@link MessageTypeResolver}; the engine's workflow metadata keys override
     * entries of the same name.
     *
     * @param stepName logical name of the publish step
     * @param payload  event payload
     * @param metadata metadata to publish with the event
     * @return handle for the publish step
     */
    public WorkflowStepResult publish(String stepName, Object payload, Metadata metadata) {
        return publish(stepName, asEventMessage(payload, metadata));
    }

    /**
     * Publishes a business event as a durable workflow step and blocks until the step is recorded.
     *
     * @param stepName logical name of the publish step
     * @param event    event to publish
     * @see BaseWorkflowContext#awaitPublish(String, EventMessage, UnaryOperator)
     */
    public void awaitPublish(String stepName, EventMessage event) {
        awaitPublish(stepName, event, UnaryOperator.identity());
    }

    /**
     * Publishes a business event given as payload and blocks until the step is recorded.
     *
     * @param stepName logical name of the publish step
     * @param payload  event payload
     * @see #publish(String, Object)
     */
    public void awaitPublish(String stepName, Object payload) {
        awaitPublish(stepName, payload, Metadata.emptyInstance());
    }

    /**
     * Publishes a business event given as payload and metadata and blocks until the step is recorded.
     *
     * @param stepName logical name of the publish step
     * @param payload  event payload
     * @param metadata metadata to publish with the event
     * @see #publish(String, Object, Metadata)
     */
    public void awaitPublish(String stepName, Object payload, Metadata metadata) {
        awaitPublish(stepName, asEventMessage(payload, metadata));
    }

    private EventMessage asEventMessage(Object payload, Metadata metadata) {
        var type = processingContext().component(MessageTypeResolver.class).resolveOrThrow(payload);
        return new GenericEventMessage(type, payload, metadata);
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
