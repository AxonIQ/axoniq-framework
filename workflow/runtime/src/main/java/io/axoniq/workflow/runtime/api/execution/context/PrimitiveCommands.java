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
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import io.axoniq.workflow.runtime.execution.payload.PayloadReducerRegistry;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer.NAME_GLOBAL_ONLY;
import static io.axoniq.workflow.runtime.execution.payload.LocalOnlyPayloadReducer.NAME_LOCAL_ONLY;

/**
 * Commands helper.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class PrimitiveCommands {

    private PrimitiveCommands() {
        // hide instantiation.
    }

    /**
     * Constructs new blocking execute command.
     *
     * @param stepName            step name.
     * @param payload             step payload.
     * @param action              action to perform.
     * @param duration            maximum duration of execution.
     * @param type                type of return object.
     * @param processingContext   processing context.
     * @param eventNameCustomizer event name customizer.
     * @param <T>                 Java type of return object.
     * @return object mapped of result of the step execution.
     */
    public static <T> BlockingExecuteWithResultCommand<T> blockingLocalExecute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> payload,
            @Nonnull PayloadProcessor action,
            @Nonnull Duration duration,
            @Nonnull TypeReference<T> type,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull ProcessingContext processingContext) {
        return new BlockingExecuteWithResultCommand<>(localExecute(stepName,
                                                                   payload,
                                                                   action,
                                                                   duration,
                                                                   eventNameCustomizer,
                                                                   processingContext),
                                                      processingContext.component(EventConverter.class),
                                                      type);
    }

    public static <T> BlockingExecuteWithResultCommand<T> blockingLocalExecute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> payload,
            @Nonnull PayloadProcessor action,
            @Nonnull Duration duration,
            @Nonnull TypeReference<T> type,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull RetryPolicy retryPolicy,
            @Nonnull ProcessingContext processingContext) {
        return new BlockingExecuteWithResultCommand<>(localExecute(stepName,
                                                                   payload,
                                                                   action,
                                                                   duration,
                                                                   eventNameCustomizer,
                                                                   retryPolicy,
                                                                   processingContext),
                                                      processingContext.component(EventConverter.class),
                                                      type
        );
    }

    /**
     * Constructs a blocking wait for command.
     *
     * @param stepName            step name.
     * @param eventCondition      condition on waiting event.
     * @param duration            maximum duration of the wait.
     * @param type                type of return object.
     * @param converter           type converter.
     * @param eventNameCustomizer event name customizer.
     * @param <T>                 Java return type.
     * @return object mapped from received event.
     */
    public static <T> BlockingWaitForCommand<T> blockingWait(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition,
            @Nonnull PayloadReducer resultReducer,
            @Nonnull Duration duration,
            @Nonnull TypeReference<T> type,
            @Nonnull EventConverter converter,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {
        return new BlockingWaitForCommand<>(
                PrimitiveCommands.wait(stepName, eventCondition, resultReducer, duration, eventNameCustomizer),
                type,
                converter
        );
    }


    public static WorkflowStepResultExecuteCommand localExecute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> local,
            @Nonnull PayloadProcessor action,
            @Nonnull Duration duration,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull ProcessingContext processingContext
    ) {
        var registry = processingContext.component(PayloadReducerRegistry.class);
        return new WorkflowStepResultExecuteCommand(stepName,
                                                    local,
                                                    action,
                                                    registry.get(NAME_LOCAL_ONLY).orElseThrow(),
                                                    registry.get(NAME_GLOBAL_ONLY).orElseThrow(),
                                                    duration,
                                                    eventNameCustomizer,
                                                    RetryPolicy.NONE);
    }

    public static WorkflowStepResultExecuteCommand localExecute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> local,
            @Nonnull PayloadProcessor action,
            @Nonnull Duration duration,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull RetryPolicy retryPolicy,
            @Nonnull ProcessingContext processingContext
    ) {
        var registry = processingContext.component(PayloadReducerRegistry.class);
        return new WorkflowStepResultExecuteCommand(stepName,
                                                    local,
                                                    action,
                                                    registry.get(NAME_LOCAL_ONLY).orElseThrow(),
                                                    registry.get(NAME_GLOBAL_ONLY).orElseThrow(),
                                                    duration,
                                                    eventNameCustomizer,
                                                    retryPolicy);
    }

    public static WorkflowStepResultWaitForCommand wait(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition,
            @Nonnull PayloadReducer resultReducer,
            @Nonnull Duration duration,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {
        return new WorkflowStepResultWaitForCommand(stepName,
                                                    eventCondition,
                                                    resultReducer,
                                                    duration,
                                                    eventNameCustomizer);
    }

    @Internal
    public abstract static class DelegatingExecuteCommand<T> implements ExecutePrimitive.ExecuteCommand<T> {

        private final WorkflowStepResultExecuteCommand delegate;

        public DelegatingExecuteCommand(WorkflowStepResultExecuteCommand delegate) {
            this.delegate = delegate;
        }

        @Nonnull
        @Override
        public String stepName() {
            return delegate.stepName();
        }

        @Nonnull
        @Override
        public Map<String, Object> local() {
            return delegate.local();
        }

        @Nonnull
        @Override
        public PayloadProcessor action() {
            return delegate.action();
        }

        @Nonnull
        @Override
        public PayloadReducer parameterPayloadReducer() {
            return delegate.parameterPayloadReducer();
        }

        @Nonnull
        @Override
        public PayloadReducer resultPayloadReducer() {
            return delegate.resultPayloadReducer();
        }

        @Nonnull
        @Override
        public Duration timeout() {
            return delegate.timeout();
        }

        @Nonnull
        @Override
        public EventNameCustomizer eventNameCustomizer() {
            return delegate.eventNameCustomizer();
        }

        @Nonnull
        @Override
        public RetryPolicy retryPolicy() {
            return delegate.retryPolicy();
        }
    }

    @Internal
    public abstract static class DelegatingWaitForCommand<T> implements WaitForPrimitive.WaitForCommand<T> {

        private final WorkflowStepResultWaitForCommand delegate;

        public DelegatingWaitForCommand(WorkflowStepResultWaitForCommand delegate) {
            this.delegate = delegate;
        }

        @Nonnull
        @Override
        public String stepName() {
            return delegate.stepName();
        }

        @Nonnull
        @Override
        public Duration timeout() {
            return delegate.timeout();
        }

        @Nonnull
        @Override
        public EventCondition eventCondition() {
            return delegate.eventCondition();
        }

        @Nonnull
        @Override
        public EventNameCustomizer eventNameCustomizer() {
            return delegate.eventNameCustomizer();
        }

        @Nonnull
        @Override
        public PayloadReducer resultPayloadReducer() {
            return delegate.resultPayloadReducer();
        }
    }

    /**
     * Default execute command implementation.
     */
    @Internal
    public record WorkflowStepResultExecuteCommand(
            String stepName,
            Map<String, Object> local,
            PayloadProcessor action,
            PayloadReducer parameterMapping,
            PayloadReducer resultMapping,
            Duration timeout,
            EventNameCustomizer eventNameCustomizer,
            RetryPolicy retryPolicy
    ) implements ExecutePrimitive.ExecuteCommand<WorkflowStepResult> {

        public WorkflowStepResultExecuteCommand(
                @Nonnull String stepName,
                @Nonnull Map<String, Object> local,
                @Nonnull PayloadProcessor action,
                @Nonnull PayloadReducer parameterMapping,
                @Nonnull PayloadReducer resultMapping,
                @Nonnull Duration timeout,
                @Nonnull EventNameCustomizer eventNameCustomizer,
                @Nonnull RetryPolicy retryPolicy
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

        @Nonnull
        @Override
        public String stepName() {
            return stepName;
        }

        @Nonnull
        @Override
        public Map<String, Object> local() {
            return local;
        }

        @Nonnull
        @Override
        public PayloadProcessor action() {
            return action;
        }

        @Nonnull
        @Override
        public PayloadReducer parameterPayloadReducer() {
            return parameterMapping;
        }

        @Nonnull
        @Override
        public PayloadReducer resultPayloadReducer() {
            return resultMapping;
        }

        @Nonnull
        @Override
        public Duration timeout() {
            return timeout;
        }

        @Nonnull
        @Override
        public EventNameCustomizer eventNameCustomizer() {
            return eventNameCustomizer;
        }

        @Nonnull
        @Override
        public RetryPolicy retryPolicy() {
            return retryPolicy;
        }

        @Override
        public WorkflowStepResult result(@Nonnull WorkflowStepResult result) {
            return result;
        }
    }

    @Internal
    public record WorkflowStepResultWaitForCommand(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition,
            @Nonnull PayloadReducer resultPayloadReducer,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) implements WaitForPrimitive.WaitForCommand<WorkflowStepResult> {

        @Nonnull
        @Override
        public String stepName() {
            return stepName;
        }

        @Nonnull
        @Override
        public EventCondition eventCondition() {
            return eventCondition;
        }

        @Nonnull
        @Override
        public Duration timeout() {
            return timeout;
        }

        @Nonnull
        @Override
        public EventNameCustomizer eventNameCustomizer() {
            return eventNameCustomizer;
        }

        @Override
        public WorkflowStepResult result(@Nonnull WorkflowStepResult result) {
            return result;
        }
    }

    /**
     * Blocking execute with result command.
     *
     * @param <T> type of result.
     * @author Simon Zambrovski
     * @since 1.0.0
     */
    @Internal
    public static class BlockingExecuteWithResultCommand<T> extends PrimitiveCommands.DelegatingExecuteCommand<T> {

        private final EventConverter converter;
        private final TypeReference<T> type;


        @Internal
        BlockingExecuteWithResultCommand(
                @Nonnull PrimitiveCommands.WorkflowStepResultExecuteCommand command,
                @Nonnull EventConverter converter,
                @Nonnull TypeReference<T> type) {
            super(command);
            this.converter = converter;
            this.type = type;
        }

        @Override
        public T result(@Nonnull WorkflowStepResult result) {
            if (result.success() && result.<Map<String, Object>>result().isPresent()) {
                Map<String, Object> resultPayload = result.<Map<String, Object>>result().get();
                return converter.convert(resultPayload, type.getType());
            } else {
                throw result.error().orElseThrow();
            }
        }
    }

    /**
     * Blocking wait for command.
     *
     * @param <T> type of event.
     * @author Simon Zambrovski
     * @since 1.0.0
     */
    @Internal
    public static class BlockingWaitForCommand<T> extends PrimitiveCommands.DelegatingWaitForCommand<T> {

        private final EventConverter converter;
        private final TypeReference<T> type;

        @Internal
        BlockingWaitForCommand(
                @Nonnull PrimitiveCommands.WorkflowStepResultWaitForCommand command,
                @Nonnull TypeReference<T> type,
                @Nonnull EventConverter converter
        ) {
            super(command);
            this.converter = converter;
            this.type = type;
        }

        @Override
        public T result(@Nonnull WorkflowStepResult result) {
            if (result.success() && result.<Map<String, Object>>result().isPresent()) {
                Map<String, Object> resultPayload = result.<Map<String, Object>>result().get();
                return converter.convert(resultPayload, type.getType());
            } else {
                throw result.error().orElseThrow();
            }
        }
    }
}
