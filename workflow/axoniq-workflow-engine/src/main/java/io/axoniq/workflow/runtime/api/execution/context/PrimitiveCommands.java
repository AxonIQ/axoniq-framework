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
package io.axoniq.workflow.runtime.api.execution.context;

import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.payload.PayloadModification;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Commands helper.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class PrimitiveCommands {

    private PrimitiveCommands() {
        // hide instantiation.
    }

    /**
     * Factory method to create a {@link WorkflowStepResultWaitForCommand}.
     *
     * @param stepName             step name
     * @param eventCondition       event condition
     * @param resultPayloadReducer result payload reducer
     * @param duration             timeout duration
     * @param eventNameCustomizer  event name customizer
     * @return command object
     */
    public static WorkflowStepResultWaitForCommand waitForEvent(
            String stepName,
            EventCondition eventCondition,
            PayloadReducer resultPayloadReducer,
            Duration duration,
            EventNameCustomizer eventNameCustomizer
    ) {
        return new WorkflowStepResultWaitForCommand(stepName,
                                                    eventCondition,
                                                    resultPayloadReducer,
                                                    duration,
                                                    eventNameCustomizer);
    }

    /**
     * Factory method to create a {@link WorkflowStepResultModifyPayloadCommand}.
     *
     * @param stepName            step name
     * @param payloadModification payload modification
     * @param eventNameCustomizer event name customizer
     * @return command object
     */
    public static WorkflowStepResultModifyPayloadCommand modifyPayload(
            String stepName,
            PayloadModification payloadModification,
            EventNameCustomizer eventNameCustomizer
    ) {
        return new WorkflowStepResultModifyPayloadCommand(stepName, payloadModification, eventNameCustomizer);
    }

    /**
     * Factory method to create a {@link SimpleVersionCommand}.
     *
     * @param stepName            logical step name (developer-chosen identifier describing the change).
     * @param newVersion          new workflow version to record (semver string).
     * @param eventNameCustomizer event name customizer.
     * @return command object.
     */
    public static SimpleVersionCommand version(
            String stepName,
            String newVersion,
            EventNameCustomizer eventNameCustomizer
    ) {
        return new SimpleVersionCommand(stepName, newVersion, eventNameCustomizer);
    }

    /**
     * Creates a command cancelling an entire workflow.
     *
     * @param cause               optional cancellation cause
     * @param eventNameCustomizer customizer for published event names
     * @return workflow cancellation command
     */
    public static WorkflowLifecycleControl.CancelWorkflowCommand cancelWorkflow(
            @Nullable Throwable cause,
            EventNameCustomizer eventNameCustomizer
    ) {
        return new DefaultCancelWorkflowCommand(cause, eventNameCustomizer);
    }

    /**
     * Creates a command failing an entire workflow.
     *
     * @param cause               optional failure cause
     * @param eventNameCustomizer customizer for published event names
     * @return workflow failure command
     */
    public static WorkflowLifecycleControl.FailWorkflowCommand failWorkflow(
            @Nullable Throwable cause,
            EventNameCustomizer eventNameCustomizer
    ) {
        return new DefaultFailWorkflowCommand(cause, eventNameCustomizer);
    }

    /**
     * Creates a command cancelling one running step.
     *
     * @param stepName            logical name of the step
     * @param cause               optional cancellation cause
     * @param eventNameCustomizer customizer for published event names
     * @return step cancellation command
     */
    public static WorkflowLifecycleControl.CancelStepCommand cancelStep(
            String stepName,
            @Nullable Throwable cause,
            EventNameCustomizer eventNameCustomizer
    ) {
        return new DefaultCancelStepCommand(stepName, cause, eventNameCustomizer);
    }

    /**
     * Default execute command implementation.
     */
    @Internal
    public record WorkflowStepResultExecuteCommand(
            String stepName,
            Map<String, @Nullable Object> local,
            PayloadProcessor action,
            PayloadReducer parameterMapping,
            PayloadReducer resultMapping,
            Duration timeout,
            EventNameCustomizer eventNameCustomizer,
            RetryPolicy retryPolicy
    ) implements ExecutePrimitive.ExecuteCommand {

        public WorkflowStepResultExecuteCommand(
                String stepName,
                Map<String, @Nullable Object> local,
                PayloadProcessor action,
                PayloadReducer parameterMapping,
                PayloadReducer resultMapping,
                Duration timeout,
                EventNameCustomizer eventNameCustomizer,
                RetryPolicy retryPolicy
        ) {
            this.stepName = stepName;
            this.local = local;
            this.action = action;
            this.parameterMapping = parameterMapping;
            this.resultMapping = resultMapping;
            this.timeout = timeout;
            this.eventNameCustomizer = eventNameCustomizer;
            this.retryPolicy = retryPolicy;
        }

        @Override
        public String stepName() {
            return stepName;
        }

        @Override
        public Map<String, @Nullable Object> local() {
            return local;
        }

        @Override
        public PayloadProcessor action() {
            return action;
        }

        @Override
        public PayloadReducer parameterPayloadReducer() {
            return parameterMapping;
        }

        @Override
        public PayloadReducer resultPayloadReducer() {
            return resultMapping;
        }

        @Override
        public Duration timeout() {
            return timeout;
        }

        @Override
        public EventNameCustomizer eventNameCustomizer() {
            return eventNameCustomizer;
        }

        @Override
        public RetryPolicy retryPolicy() {
            return retryPolicy;
        }
    }

    /**
     * Default wait-for-event command implementation allowing asynchronous execution and returning the
     * {@link WorkflowStepResult}.
     *
     * @param stepName             step name
     * @param eventCondition       event condition
     * @param resultPayloadReducer result payload reducer
     * @param timeout              timeout duration
     * @param eventNameCustomizer  event name customizer
     */
    @Internal
    public record WorkflowStepResultWaitForCommand(
            String stepName,
            EventCondition eventCondition,
            PayloadReducer resultPayloadReducer,
            Duration timeout,
            EventNameCustomizer eventNameCustomizer
    ) implements WaitForPrimitive.WaitForCommand {
    }

    /**
     * Default payload modification command implementation allowing asynchronous execution and returning the
     * {@link WorkflowStepResult}.
     *
     * @param stepName            step name
     * @param payloadModification payload modification
     * @param eventNameCustomizer event name customizer
     */
    @Internal
    public record WorkflowStepResultModifyPayloadCommand(
            String stepName,
            PayloadModification payloadModification,
            EventNameCustomizer eventNameCustomizer
    ) implements PayloadPrimitive.ModifyPayloadCommand {
    }

    /**
     * Default version command implementation.
     */
    @Internal
    public record SimpleVersionCommand(
            String stepName,
            String newVersion,
            EventNameCustomizer eventNameCustomizer
    ) implements VersionPrimitive.VersionCommand {
    }

    /**
     * Default command implementation for whole-workflow cancellation.
     */
    @Internal
    record DefaultCancelWorkflowCommand(
            @Nullable Throwable cause,
            EventNameCustomizer eventNameCustomizer
    ) implements WorkflowLifecycleControl.CancelWorkflowCommand {

        DefaultCancelWorkflowCommand {
            Objects.requireNonNull(eventNameCustomizer, "EventNameCustomizer is required");
        }
    }

    /**
     * Default command implementation for whole-workflow failure.
     */
    @Internal
    record DefaultFailWorkflowCommand(
            @Nullable Throwable cause,
            EventNameCustomizer eventNameCustomizer
    ) implements WorkflowLifecycleControl.FailWorkflowCommand {

        DefaultFailWorkflowCommand {
            Objects.requireNonNull(eventNameCustomizer, "EventNameCustomizer is required");
        }
    }

    /**
     * Default command implementation for single-step cancellation.
     */
    @Internal
    record DefaultCancelStepCommand(
            String stepName,
            @Nullable Throwable cause,
            EventNameCustomizer eventNameCustomizer
    ) implements WorkflowLifecycleControl.CancelStepCommand {

        DefaultCancelStepCommand {
            Objects.requireNonNull(stepName, "Step name is required");
            Objects.requireNonNull(eventNameCustomizer, "EventNameCustomizer is required");
        }
    }
}
