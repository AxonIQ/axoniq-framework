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
package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.dsl.Payload;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.PayloadProcessor;
import io.axoniq.workflow.runtime.api.PayloadReducer;
import io.axoniq.workflow.runtime.api.TerminatePrimitive;
import io.axoniq.workflow.runtime.api.TerminatePrimitive.TerminateCommand;
import io.axoniq.workflow.runtime.api.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.impl.WorkflowInstance;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.axoniq.workflow.dsl.Payload.payload;
import static io.axoniq.workflow.runtime.api.PayloadReducer.local;
import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName;


public class SimpleWorkflowContext extends WorkflowInstance
        implements WaitForPrimitive, ExecutePrimitive, TerminatePrimitive {

    public SimpleWorkflowContext(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull ProcessingContext processingContext,
            @Nonnull EventNameCustomizer parentCustomizer
    ) {
        super(workflowId, payload, processingContext, parentCustomizer);
    }

    public <T> T awaitEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout) {
        return waitFor(BlockingWaitForCommand.blocking(
                stepName,
                super.processingContext().component(MessageTypeResolver.class).resolve(eventType).orElseThrow()
                     .qualifiedName(),
                e -> predicate.test(e.payloadAs(eventType)),
                timeout,
                TypeReference.fromType(eventType),
                super.processingContext().component(Converter.class)
        ));
    }

    public <T> T awaitEvent(String stepName, Class<T> eventType, Duration timeout) {
        return this.awaitEvent(stepName, eventType, e -> true, timeout);
    }

    public <T> T awaitEvent(String stepName, Class<T> eventType) {
        return this.awaitEvent(stepName, eventType, Duration.ofSeconds(5));
    }

    public void block(String stepName, Duration timeout) {
        var result = waitFor(stepName, new QualifiedName(Void.class), (e) -> false, timeout, eventName());
        if (result.isFailure() && result.error().isPresent()) {
            throw result.error().get();
        }
    }

    public WorkflowStepResult execute(String stepName, Map<String, Object> payload, PayloadProcessor action,
                                      Duration duration) {
        return execute(stepName, payload, action, local(), PayloadReducer.all(), duration, eventName());
    }


    public Map<String, Object> awaitExecute(String stepName, Map<String, Object> payload, PayloadProcessor action,
                                            Duration timeout) {
        return execute(
                BlockingExecuteWithResultCommand.blockingLocal(stepName,
                                                               payload,
                                                               action,
                                                               timeout,
                                                               new TypeReference<>() {
                                                               },
                                                               super.processingContext().component(Converter.class))
        );
    }

    public <T> T awaitExecute(String stepName, Map<String, Object> payload, Class<T> returnType,
                              Function<Map<String, Object>, T> action, EventNameCustomizer eventNameCustomizer) {
        var stepSpecificName = "__" + stepName;
        var command = BlockingExecuteWithResultCommand.blockingLocal(stepName, payload,
                                                                     (c, p) -> {
                                                                         var stepResult = action.apply(p);
                                                                         if (stepResult != null) {
                                                                             return Map.of(stepSpecificName,
                                                                                           stepResult);
                                                                         } else {
                                                                             return Map.of();
                                                                         }
                                                                     }
                , Duration.ofSeconds(5), new TypeReference<Map<String, Object>>() {
                }, super.processingContext().component(Converter.class));
        //noinspection unchecked
        return (T) execute(command).get(stepSpecificName);
    }

    public Map<String, Object> awaitExecute(String stepName, Map<String, Object> payload, PayloadProcessor action) {
        return this.awaitExecute(stepName, payload, action, Duration.ofSeconds(5));
    }

    public <T> T awaitExecute(String stepName, Map<String, Object> payload, Class<T> returnType,
                              Function<Map<String, Object>, T> action) {
        return this.awaitExecute(stepName, payload, returnType, action, eventName());
    }

    public <T> T awaitExecute(String stepName, Class<T> returnType, Supplier<T> action) {
        return this.awaitExecute(stepName, Map.of(), returnType, (p) -> action.get());
    }

    public void awaitExecute(String stepName, Runnable action) {
        this.awaitExecute(stepName, Void.class, () -> {
            action.run();
            return null;
        });
    }

    public <T> T awaitExecute(String stepName, Payload payload, Class<T> returnType, Function<Payload, T> action) {
        return this.awaitExecute(stepName, payload.getValues(), returnType, (m) -> action.apply(payload(m)));
    }

    /**
     * Terminates the entire workflow with an error, publishing a failure event and
     * cancelling all running steps.
     *
     * @param cause the exception that caused the failure
     * @throws io.axoniq.workflow.runtime.api.WorkflowFailedException always, after the failure event is published
     */
    public void fail(Throwable cause) {
        terminate(TerminateCommand.fail(cause, eventName()));
    }

    /**
     * Terminates the entire workflow with an error, publishing a failure event and
     * cancelling all running steps.
     *
     * @param cause                the exception that caused the failure
     * @param eventNameCustomizer  customizer for the published failure event name
     * @throws io.axoniq.workflow.runtime.api.WorkflowFailedException always, after the failure event is published
     */
    public void fail(Throwable cause, EventNameCustomizer eventNameCustomizer) {
        terminate(TerminateCommand.fail(cause, eventNameCustomizer));
    }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @throws io.axoniq.workflow.runtime.api.WorkflowCancelledException always, after the cancellation event is published
     */
    public void cancel() {
        terminate(TerminateCommand.cancel(eventName()));
    }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @param eventNameCustomizer customizer for the published cancellation event name
     * @throws io.axoniq.workflow.runtime.api.WorkflowCancelledException always, after the cancellation event is published
     */
    public void cancel(EventNameCustomizer eventNameCustomizer) {
        terminate(TerminateCommand.cancel(eventNameCustomizer));
    }

    /**
     * Cancels the entire workflow gracefully with a human-readable reason, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @param reason descriptive reason for the cancellation
     * @throws io.axoniq.workflow.runtime.api.WorkflowCancelledException always, after the cancellation event is published
     */
    public void cancel(String reason) {
        terminate(TerminateCommand.cancel(new io.axoniq.workflow.runtime.api.WorkflowCancelledException(reason), eventName()));
    }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @param cause the exception that triggered the cancellation
     * @throws io.axoniq.workflow.runtime.api.WorkflowCancelledException always, after the cancellation event is published
     */
    public void cancel(Throwable cause) {
        terminate(TerminateCommand.cancel(cause, eventName()));
    }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @param cause               the exception that triggered the cancellation
     * @param eventNameCustomizer customizer for the published cancellation event name
     * @throws io.axoniq.workflow.runtime.api.WorkflowCancelledException always, after the cancellation event is published
     */
    public void cancel(Throwable cause, EventNameCustomizer eventNameCustomizer) {
        terminate(TerminateCommand.cancel(cause, eventNameCustomizer));
    }

    /**
     * Cancels a single running step by name without terminating the workflow.
     * The step's future is completed exceptionally with a {@link io.axoniq.workflow.runtime.api.StepCancellationException}.
     *
     * @param stepName the name of the step to cancel
     */
    public void cancelStep(String stepName) {
        terminate(TerminateCommand.cancelledStep(stepName, null, eventName()));
    }

    /**
     * Cancels a single running step by name without terminating the workflow.
     * The step's future is completed exceptionally with the given cause, wrapped in a
     * {@link io.axoniq.workflow.runtime.api.StepCancellationException} if it isn't one already.
     *
     * @param stepName the name of the step to cancel
     * @param cause    the exception that caused the step cancellation
     */
    public void cancelStep(String stepName, Throwable cause) {
        terminate(TerminateCommand.cancelledStep(stepName, cause, eventName()));
    }

    /**
     * Cancels a single running step by name without terminating the workflow.
     * The step's future is completed exceptionally with a {@link io.axoniq.workflow.runtime.api.StepCancellationException}
     * carrying the given reason.
     *
     * @param stepName the name of the step to cancel
     * @param reason   descriptive reason for the step cancellation
     */
    public void cancelStep(String stepName, String reason) {
        terminate(TerminateCommand.cancelledStep(stepName, new io.axoniq.workflow.runtime.api.StepCancellationException(reason), eventName()));
    }

    public void addPayload(Object object) {
        addPayload(payload(this, object));
    }

    public void addPayload(Payload payload) {
        applyPayloadModification(p -> payload(p).with(payload).getValues());
    }
}
