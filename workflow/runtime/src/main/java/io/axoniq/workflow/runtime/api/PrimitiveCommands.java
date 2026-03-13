/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api;

import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;

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
     * @param converter           type converter.
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
            @Nonnull Converter converter,
            @Nonnull EventNameCustomizer eventNameCustomizer) {
        return new BlockingExecuteWithResultCommand<>(localExecute(stepName,
                                                                   payload,
                                                                   action,
                                                                   duration,
                                                                   eventNameCustomizer), converter, type);
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
            @Nonnull Converter converter,
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
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {
        return new WorkflowStepResultExecuteCommand(stepName,
                                                    local,
                                                    action,
                                                    PayloadReducer.LOCAL,
                                                    PayloadReducer.CONTEXT,
                                                    duration,
                                                    eventNameCustomizer);
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

        @NotNull
        @Override
        public String stepName() {
            return delegate.stepName();
        }

        @NotNull
        @Override
        public Map<String, Object> local() {
            return delegate.local();
        }

        @NotNull
        @Override
        public PayloadProcessor action() {
            return delegate.action();
        }

        @NotNull
        @Override
        public PayloadReducer parameterMapping() {
            return delegate.parameterMapping();
        }

        @NotNull
        @Override
        public PayloadReducer resultMapping() {
            return delegate.resultMapping();
        }

        @NotNull
        @Override
        public Duration timeout() {
            return delegate.timeout();
        }

        @NotNull
        @Override
        public EventNameCustomizer eventNameCustomizer() {
            return delegate.eventNameCustomizer();
        }
    }

    @Internal
    public abstract static class DelegatingWaitForCommand<T> implements WaitForPrimitive.WaitForCommand<T> {

        private final WorkflowStepResultWaitForCommand delegate;

        public DelegatingWaitForCommand(WorkflowStepResultWaitForCommand delegate) {
            this.delegate = delegate;
        }

        @NotNull
        @Override
        public String stepName() {
            return delegate.stepName();
        }

        @NotNull
        @Override
        public Duration timeout() {
            return delegate.timeout();
        }

        @Nonnull
        @Override
        public EventCondition eventCondition() {
            return delegate.eventCondition();
        }

        @NotNull
        @Override
        public EventNameCustomizer eventNameCustomizer() {
            return delegate.eventNameCustomizer();
        }

        @Nonnull
        @Override
        public PayloadReducer resultReducer() {
            return delegate.resultReducer();
        }
    }

    /**
     * Default execute command implementation.
     */
    @Internal
    public record WorkflowStepResultExecuteCommand(String stepName, Map<String, Object> local, PayloadProcessor action,
                                                   PayloadReducer parameterMapping, PayloadReducer resultMapping,
                                                   Duration timeout, EventNameCustomizer eventNameCustomizer)
            implements ExecutePrimitive.ExecuteCommand<WorkflowStepResult> {

        public WorkflowStepResultExecuteCommand(
                @Nonnull String stepName,
                @Nonnull Map<String, Object> local,
                @Nonnull PayloadProcessor action,
                @Nonnull PayloadReducer parameterMapping,
                @Nonnull PayloadReducer resultMapping,
                @Nonnull Duration timeout,
                @Nonnull EventNameCustomizer eventNameCustomizer
        ) {
            this.stepName = stepName;
            this.local = local;
            this.action = action;
            this.parameterMapping = parameterMapping;
            this.resultMapping = resultMapping;
            this.timeout = timeout;
            this.eventNameCustomizer = eventNameCustomizer;
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
        public PayloadReducer parameterMapping() {
            return parameterMapping;
        }

        @Nonnull
        @Override
        public PayloadReducer resultMapping() {
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

        @Override
        public WorkflowStepResult result(@Nonnull WorkflowStepResult result) {
            return result;
        }
    }

    @Internal
    public record WorkflowStepResultWaitForCommand(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition,
            @Nonnull PayloadReducer resultReducer,
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

        private final Converter converter;
        private final TypeReference<T> type;


        @Internal
        BlockingExecuteWithResultCommand(
                @Nonnull PrimitiveCommands.WorkflowStepResultExecuteCommand command,
                @Nonnull Converter converter,
                @Nonnull TypeReference<T> type) {
            super(command);
            this.converter = converter;
            this.type = type;
        }

        @Override
        public T result(@NotNull WorkflowStepResult result) {
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

        private final Converter converter;
        private final TypeReference<T> type;

        @Internal
        BlockingWaitForCommand(
                @Nonnull PrimitiveCommands.WorkflowStepResultWaitForCommand command,
                @Nonnull TypeReference<T> type,
                @Nonnull Converter converter
        ) {
            super(command);
            this.converter = converter;
            this.type = type;
        }

        @Override
        public T result(@NotNull WorkflowStepResult result) {
            if (result.success() && result.<Map<String, Object>>result().isPresent()) {
                Map<String, Object> resultPayload = result.<Map<String, Object>>result().get();
                return converter.convert(resultPayload, type.getType());
            } else {
                throw result.error().orElseThrow();
            }
        }
    }
}
