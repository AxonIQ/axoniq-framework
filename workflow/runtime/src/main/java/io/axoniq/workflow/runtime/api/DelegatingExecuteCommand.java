package io.axoniq.workflow.runtime.api;

import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;

public abstract class DelegatingExecuteCommand<T> implements ExecutePrimitive.ExecuteCommand<T> {

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
