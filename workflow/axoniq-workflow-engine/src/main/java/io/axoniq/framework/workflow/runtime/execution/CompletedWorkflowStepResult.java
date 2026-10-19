/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.PAYLOAD_TYPE;

/**
 * A step execution result that already has completed.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
class CompletedWorkflowStepResult implements WorkflowStepResult {

    private final String stepName;
    private final Object payload;
    private final StepFailedException error;
    private final Duration timeout;
    private final boolean cancelled;
    private final Converter resultConverter;

    CompletedWorkflowStepResult(String stepName,
                                @Nullable Object payload,
                                @Nullable Throwable error,
                                @Nullable Duration timeout,
                                boolean cancelled,
                                @Nullable EventConverter resultConverter) {
        this.stepName = stepName;
        this.payload = payload;
        this.resultConverter = resultConverter;
        if (error != null) {
            if (error instanceof StepFailedException) {
                this.error = (StepFailedException) error;
            } else {
                this.error = new StepFailedException(error);
            }
        } else {
            this.error = null;
        }
        this.timeout = timeout;
        this.cancelled = cancelled;
    }

    @Override
    public String getStepName() {
        return stepName;
    }

    @Override
    public boolean isCompleted() {
        return true;
    }

    @Override
    public Optional<Map<String, @Nullable Object>> result() {
        return Optional.ofNullable(payload)
                       .flatMap(p -> Optional.ofNullable(resultConverter)
                                             .map(c -> c.convert(p, PAYLOAD_TYPE.getType())));
    }

    @Override
    public <T> Optional<T> resultAs(Type type) {
        return Optional.ofNullable(payload)
                       .flatMap(p -> Optional.ofNullable(resultConverter)
                                             .map(c -> c.convert(p, type)));
    }

    @Override
    public <T> Optional<T> resultAs(Type type, Converter converter) {
        return Optional.ofNullable(payload).map(p -> converter.convert(p, type));
    }

    @Override
    public Optional<StepFailedException> error() {
        return Optional.ofNullable(error);
    }

    @Override
    public boolean success() {
        return !cancelled && timeout == null && error == null;
    }

    @Override
    public boolean failure() {
        return error != null;
    }

    @Override
    public boolean canceled() {
        return cancelled;
    }

    @Override
    public boolean timeout() {
        return timeout != null;
    }

    @Override
    public void await() {
    }

    @Override
    public void cancel() {
        // no-op: already in terminal state
    }

    @Override
    public void cancel(String reason) {
        // no-op: already in terminal state
    }
}
