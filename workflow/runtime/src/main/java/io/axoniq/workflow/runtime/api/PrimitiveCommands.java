package io.axoniq.workflow.runtime.api;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName;

public class PrimitiveCommands {

    private PrimitiveCommands() {

    }

    public static WorkflowStepResultExecuteCommand localExecute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> local,
            @Nonnull PayloadProcessor action,
            @Nonnull Duration timeout
    ) {
        return new WorkflowStepResultExecuteCommand(stepName,
                                                    local,
                                                    action,
                                                    PayloadReducer.local(),
                                                    PayloadReducer.all(),
                                                    timeout,
                                                    eventName());
    }

    public static WorkflowStepResultWaitForCommand waitFor(
            @Nonnull String stepName,
            @Nonnull QualifiedName qualifiedName,
            @Nonnull Predicate<EventMessage> predicate,
            @Nonnull Duration timeout
    ) {
        return new WorkflowStepResultWaitForCommand(stepName, qualifiedName, predicate, timeout, eventName());
    }

    public static WorkflowStepResultWaitForCommand wait(
            @Nonnull String stepName,
            @Nonnull Duration timeout
    ) {
        return new WorkflowStepResultWaitForCommand(stepName,
                                                    new QualifiedName(Void.class),
                                                    e -> false,
                                                    timeout,
                                                    eventName());
    }

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
        public QualifiedName qualifiedName() {
            return delegate.qualifiedName();
        }

        @Nonnull
        @Override
        public Predicate<EventMessage> predicate() {
            return delegate.predicate();
        }

        @NotNull
        @Override
        public EventNameCustomizer eventNameCustomizer() {
            return delegate.eventNameCustomizer();
        }
    }

    /**
     * Default execute command implementation.
     */
    public static class WorkflowStepResultExecuteCommand implements ExecutePrimitive.ExecuteCommand<WorkflowStepResult> {

        private final String stepName;
        private final Map<String, Object> local;
        private final PayloadProcessor action;
        private final PayloadReducer parameterMapping;
        private final PayloadReducer resultMapping;
        private final Duration timeout;
        private final EventNameCustomizer eventNameCustomizer;

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

    public static class WorkflowStepResultWaitForCommand implements WaitForPrimitive.WaitForCommand<WorkflowStepResult> {

        private final String stepName;
        private final Duration timeout;
        private final EventNameCustomizer eventNameCustomizer;
        private final QualifiedName qualifiedName;
        private final Predicate<EventMessage> predicate;

        public WorkflowStepResultWaitForCommand(
                @Nonnull String stepName,
                @Nonnull QualifiedName qualifiedName,
                @Nonnull Predicate<EventMessage> predicate,
                @Nonnull Duration timeout,
                @Nonnull EventNameCustomizer eventNameCustomizer
        ) {
            this.stepName = stepName;
            this.timeout = timeout;
            this.qualifiedName = qualifiedName;
            this.predicate = predicate;
            this.eventNameCustomizer = eventNameCustomizer;
        }

        @Nonnull
        @Override
        public String stepName() {
            return stepName;
        }

        @Nonnull
        @Override
        public QualifiedName qualifiedName() {
            return qualifiedName;
        }

        @Nonnull
        @Override
        public Predicate<EventMessage> predicate() {
            return predicate;
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
}
