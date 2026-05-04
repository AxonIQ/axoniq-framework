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

import io.axoniq.workflow.dsl.api.AssociationsUtils;
import io.axoniq.workflow.dsl.api.Payload;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.association.EqualsComparison;
import io.axoniq.workflow.runtime.execution.AbstractDSLWorkflowContext;
import io.axoniq.workflow.runtime.execution.payload.PayloadReducerRegistry;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.axoniq.workflow.dsl.api.Payload.payload;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static io.axoniq.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer.NAME_COMBINE_GLOBAL_AND_LOCAL;
import static io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer.NAME_GLOBAL_ONLY;
import static io.axoniq.workflow.runtime.execution.payload.LocalOnlyPayloadReducer.NAME_LOCAL_ONLY;

/**
 * Simple workflow DSL providing synchronous versions of step primitives.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Allrad Buijze
 * @author Steven van Beelen
 * @since 1.0.0
 */
public class SimpleWorkflowContext extends AbstractDSLWorkflowContext {

    protected final PayloadReducerRegistry registry;
    private Duration defaultTimeout;

    /**
     * Creates an equals matcher for association values.
     *
     * @param value value to match inside the event.
     * @return equals value matcher.
     */
    public static AssociationsUtils.VariableMatcher equalsTo(Object value) {
        return new AssociationsUtils.VariableMatcher(EqualsComparison.OPERATOR, value);
    }


    /**
     * Constructs the simple context. This parameter is called by the corresponding workflow context factory, see
     * {@link SimpleWorkflowContextFactory}.
     *
     * @param workflowId            workflow id.
     * @param payload               initial payload.
     * @param processingContext     processing context.
     * @param workflowConfiguration workflow configuration.
     */
    public SimpleWorkflowContext(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull ProcessingContext processingContext,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration
    ) {
        super(workflowId, payload, processingContext, workflowConfiguration);
        defaultTimeout = Duration.ofSeconds(5);
        registry = super.processingContext().component(PayloadReducerRegistry.class);
    }

    /**
     * Asynchronous wait for an event to be published.
     *
     * @param stepName  name of the step.
     * @param eventType type of event.
     * @param predicate condition on event.
     * @param timeout   maximum time to wait.
     * @param <T>       type of the event.
     * @return workflow step result.
     */
    public <T> WorkflowStepResult waitFor(
            String stepName,
            Class<T> eventType,
            Predicate<T> predicate,
            Duration timeout) {
        return waitFor(
                stepName,
                EventConditions.fromQualifiedName(
                        super.processingContext().component(MessageTypeResolver.class).resolve(eventType).orElseThrow()
                             .qualifiedName(),
                        e -> predicate.test(e.payloadAs(eventType))
                ),
                registry.get(NAME_GLOBAL_ONLY).orElseThrow(),
                timeout,
                defaults()
        );
    }

    /**
     * Asynchronous wait for an event to be published.
     *
     * @param stepName          name of the step.
     * @param eventType         event type.
     * @param associationsUtils condition on event encapsulated in an {@link AssociationsUtils} instance.
     * @param timeout           maximum time to wait.
     * @param <T>               type of the event.
     * @return workflow step result.
     */
    public <T> WorkflowStepResult waitFor(
            String stepName,
            Class<T> eventType,
            AssociationsUtils associationsUtils,
            Duration timeout) {
        return waitFor(
                stepName,
                EventConditions.fromQualifiedName(
                        super.processingContext()
                             .component(MessageTypeResolver.class).resolve(eventType).orElseThrow().qualifiedName(),
                        e -> associationsUtils.build(super.processingContext()).test(e)
                ),
                registry.get(NAME_GLOBAL_ONLY).orElseThrow(),
                timeout,
                defaults()
        );
    }


    /**
     * Synchronous (blocking) wait for an event to be published.
     *
     * @param stepName  step name.
     * @param eventType event type.
     * @param predicate condition on event.
     * @param timeout   maximum time to wait.
     * @param <T>       type of the event.
     * @return event payload.
     */
    public <T> T awaitEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout) {
        return waitFor(PrimitiveCommands.blockingWait(
                stepName,
                EventConditions.fromQualifiedName(
                        super.processingContext().component(MessageTypeResolver.class).resolve(eventType).orElseThrow()
                             .qualifiedName(),
                        e -> predicate.test(e.payloadAs(eventType))
                ),
                registry.get(NAME_GLOBAL_ONLY).orElseThrow(),
                timeout,
                TypeReference.fromType(eventType),
                super.processingContext().component(EventConverter.class),
                defaults()
        ));
    }

    /**
     * Synchronous (blocking) wait for an event to be published.
     *
     * @param stepName          step name.
     * @param eventType         event type.
     * @param associationsUtils condition on event encapsulated in an {@link AssociationsUtils} instance.
     * @param timeout           maximum time to wait.
     * @param <T>               type of the event.
     * @return event payload.
     */
    public <T> T awaitEvent(String stepName, Class<T> eventType, AssociationsUtils associationsUtils,
                            Duration timeout) {
        return waitFor(PrimitiveCommands.blockingWait(
                stepName,
                EventConditions.fromQualifiedName(
                        super.processingContext()
                             .component(MessageTypeResolver.class).resolve(eventType).orElseThrow().qualifiedName(),
                        e -> associationsUtils.build(super.processingContext()).test(e)
                ),
                registry.get(NAME_GLOBAL_ONLY).orElseThrow(),
                timeout,
                TypeReference.fromType(eventType),
                super.processingContext().component(EventConverter.class),
                defaults()
        ));
    }

    /**
     * Synchronous (blocking) wait for an event to be published.
     *
     * @param stepName  step name.
     * @param eventType event type.
     * @param timeout   maximum time to wait.
     * @param <T>       type of the event.
     * @return event payload.
     */
    public <T> T awaitEvent(String stepName, Class<T> eventType, Duration timeout) {
        return this.awaitEvent(stepName, eventType, e -> true, timeout);
    }

    /**
     * Non-blocking wait for an event of the given type. Returns immediately with a {@link WorkflowStepResult} that
     * completes when the event arrives or the timeout expires.
     *
     * @param stepName  name of the workflow step.
     * @param eventType the event type to wait for.
     * @param timeout   maximum wait duration.
     * @return workflow step result.
     */
    public WorkflowStepResult waitForEvent(String stepName, Class<?> eventType, Duration timeout) {
        return waitFor(stepName,
                       EventConditions.fromQualifiedName(
                               super.processingContext().component(MessageTypeResolver.class).resolve(eventType)
                                    .orElseThrow()
                                    .qualifiedName()
                       ),
                       registry.get(NAME_GLOBAL_ONLY).orElseThrow(), timeout, defaults());
    }

    /**
     * Non-blocking wait for an event of the given type with a predicate filter. Returns immediately with a
     * {@link WorkflowStepResult} that completes when a matching event arrives or the timeout expires.
     *
     * @param stepName  name of the workflow step.
     * @param eventType the event type to wait for.
     * @param predicate predicate to filter events.
     * @param timeout   maximum wait duration.
     * @param <T>       event type.
     * @return workflow step result.
     */
    public <T> WorkflowStepResult waitForEvent(String stepName, Class<T> eventType,
                                               Predicate<T> predicate, Duration timeout) {
        return waitFor(stepName,
                       EventConditions.fromQualifiedName(
                               super.processingContext().component(MessageTypeResolver.class).resolve(eventType)
                                    .orElseThrow()
                                    .qualifiedName(),
                               e -> predicate.test(e.payloadAs(eventType))
                       ),
                       registry.get(NAME_GLOBAL_ONLY).orElseThrow(), timeout, defaults());
    }

    /**
     * Non-blocking wait for an event of the given type with association filtering. Returns immediately with a
     * {@link WorkflowStepResult} that completes when a matching event arrives or the timeout expires.
     *
     * @param stepName          name of the workflow step.
     * @param eventType         the event type to wait for.
     * @param associationsUtils association filter to correlate events to this workflow instance.
     * @param timeout           maximum wait duration.
     * @return workflow step result.
     */
    public WorkflowStepResult waitForEvent(String stepName, Class<?> eventType,
                                           AssociationsUtils associationsUtils, Duration timeout) {
        return waitFor(stepName,
                       EventConditions.fromQualifiedName(
                               super.processingContext().component(MessageTypeResolver.class).resolve(eventType)
                                    .orElseThrow()
                                    .qualifiedName(),
                               e -> associationsUtils.build(super.processingContext()).test(e)
                       ),
                       registry.get(NAME_GLOBAL_ONLY).orElseThrow(), timeout, defaults());
    }

    /**
     * Blocks the execution until the timeout occurs (synchronous call).
     *
     * @param stepName name of the step.
     * @param timeout  timeout to wait.
     */
    public void sleep(String stepName, Duration timeout) {
        sleep(stepName, timeout, defaults());
    }

    /**
     * Blocks the execution until the timeout occurs (synchronous call).
     *
     * @param stepName            name of the step.
     * @param timeout             timeout to wait.
     * @param eventNameCustomizer event name customizer.
     */
    public void sleep(String stepName, Duration timeout, EventNameCustomizer eventNameCustomizer) {
        var result = waitFor(stepName,
                             EventConditions.never(),
                             registry.get(NAME_GLOBAL_ONLY).orElseThrow(),
                             timeout,
                             defaults()
        );
        if (result.failure() && result.error().isPresent()) {
            throw result.error().get();
        }
    }

    /**
     * Non-blocking sleep that returns immediately with a {@link WorkflowStepResult}. The result completes when the
     * timeout expires.
     *
     * @param stepName name of the step.
     * @param timeout  duration to sleep.
     * @return workflow step result that completes when the timeout expires.
     */
    public WorkflowStepResult sleepAsync(String stepName, Duration timeout) {
        return sleepAsync(stepName, timeout, defaults());
    }

    /**
     * Non-blocking sleep that returns immediately with a {@link WorkflowStepResult}.
     * The result completes when the timeout expires.
     *
     * @param stepName            name of the step.
     * @param timeout             duration to sleep.
     * @param eventNameCustomizer event name customizer.
     * @return workflow step result that completes when the timeout expires.
     */
    public WorkflowStepResult sleepAsync(String stepName, Duration timeout, EventNameCustomizer eventNameCustomizer) {
        return waitFor(stepName, EventConditions.never(), registry.get(NAME_GLOBAL_ONLY).orElseThrow(), timeout, defaults());
    }

    /**
     * Execute a step asynchronously.
     * <p>The payload passed will be used as step input, and the result will not be added to the workflow instance
     * payload.</p>
     *
     * @param stepName            name of the step.
     * @param payload             payload to be passed to a step.
     * @param action              action to be executed.
     * @param duration            maximum time of execution.
     * @param eventNameCustomizer event name customizer.
     * @return workflow step result.
     * @see SimpleWorkflowContext#awaitExecute(String, Map, PayloadProcessor, Duration, EventNameCustomizer) for
     * synchronous version.
     */
    public WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> payload,
            @Nonnull PayloadProcessor action,
            @Nonnull Duration duration,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {
        return execute(stepName,
                       payload,
                       action,
                       registry.get(NAME_LOCAL_ONLY).orElseThrow(),
                       registry.get(NAME_GLOBAL_ONLY).orElseThrow(),
                       duration,
                       eventNameCustomizer);
    }

    /**
     * Execute a step asynchronously using default timeout and event names.
     * <p>The payload passed will be used as step input, and the result will be not added to the workflow instance
     * payload.</p>
     *
     * @param stepName name of the step.
     * @param payload  payload to be passed to a step.
     * @param action   action to be executed.
     * @return workflow step result.
     * @see SimpleWorkflowContext#awaitExecute(String, Map, PayloadProcessor, Duration, EventNameCustomizer) for
     * synchronous version.
     */
    public WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> payload,
            @Nonnull PayloadProcessor action
    ) {
        return execute(stepName,
                       payload,
                       action,
                       registry.get(NAME_LOCAL_ONLY).orElseThrow(),
                       registry.get(NAME_GLOBAL_ONLY).orElseThrow(),
                       defaultTimeout,
                       defaults()
        );
    }

    /**
     * Execute a step asynchronously with retry support.
     *
     * @param stepName            name of the step.
     * @param payload             payload to be passed to a step.
     * @param action              action to be executed.
     * @param duration            maximum time of execution.
     * @param eventNameCustomizer event name customizer.
     * @param retryPolicy         retry policy for the step.
     * @return workflow step result.
     */
    public WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> payload,
            @Nonnull PayloadProcessor action,
            @Nonnull Duration duration,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull RetryPolicy retryPolicy
    ) {
        return execute(
                stepName,
                payload,
                action,
                registry.get(NAME_LOCAL_ONLY).orElseThrow(),
                registry.get(NAME_GLOBAL_ONLY).orElseThrow(),
                duration,
                eventNameCustomizer,
                retryPolicy
        );
    }

    /**
     * Execute a step asynchronously with retry support using default timeout and event names.
     *
     * @param stepName    name of the step.
     * @param payload     payload to be passed to a step.
     * @param action      action to be executed.
     * @param retryPolicy retry policy for the step.
     * @return workflow step result.
     */
    public WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> payload,
            @Nonnull PayloadProcessor action,
            @Nonnull RetryPolicy retryPolicy
    ) {
        return execute(
                stepName,
                payload,
                action,
                registry.get(NAME_LOCAL_ONLY).orElseThrow(),
                registry.get(NAME_GLOBAL_ONLY).orElseThrow(),
                defaultTimeout,
                defaults(),
                retryPolicy
        );
    }


    /**
     * Executes the step synchronously.
     * <p>The payload passed will be used as step input, and the result will not be added to the workflow instance.
     * payload.</p>
     *
     * @param stepName            name of the step.
     * @param payload             payload to be passed to a step.
     * @param action              action to be executed.
     * @param duration            maximum time of execution.
     * @param eventNameCustomizer event name customizer.
     * @return successful result of processing.
     * @see SimpleWorkflowContext#execute(String, Map, PayloadProcessor, Duration, EventNameCustomizer) for asynchronous
     * version.
     */
    public Map<String, Object> awaitExecute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> payload,
            @Nonnull PayloadProcessor action,
            @Nonnull Duration duration,
            @Nonnull EventNameCustomizer eventNameCustomizer) {
        return execute(
                PrimitiveCommands.blockingLocalExecute(stepName,
                                                       payload,
                                                       action,
                                                       duration,
                                                       new TypeReference<>() {
                                                       },
                                                       eventNameCustomizer,
                                                       super.processingContext()
                )
        );
    }

    /**
     * Executes the step synchronously using default duration and event names.
     * <p>The payload passed will be used as step input, and the result will not be added to the workflow instance
     * payload.</p>
     *
     * @param stepName name of the step.
     * @param payload  payload to be passed to a step.
     * @param action   action to be executed.
     * @return successful result of processing.
     * @see SimpleWorkflowContext#execute(String, Map, PayloadProcessor) for asynchronous version.
     */
    public Map<String, Object> awaitExecute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> payload,
            @Nonnull PayloadProcessor action) {
        return this.awaitExecute(stepName, payload, action, defaultTimeout, defaults());
    }

    /**
     * Executes the step synchronously with retry support.
     *
     * @param stepName            name of the step.
     * @param payload             payload to be passed to a step.
     * @param action              action to be executed.
     * @param duration            maximum time of execution.
     * @param eventNameCustomizer event name customizer.
     * @param retryPolicy         retry policy for the step.
     * @return successful result of processing.
     */
    public Map<String, Object> awaitExecute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> payload,
            @Nonnull PayloadProcessor action,
            @Nonnull Duration duration,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull RetryPolicy retryPolicy) {
        return execute(
                PrimitiveCommands.blockingLocalExecute(stepName,
                                                       payload,
                                                       action,
                                                       duration,
                                                       new TypeReference<>() {
                                                       },
                                                       eventNameCustomizer,
                                                       retryPolicy,
                                                       super.processingContext()

                )
        );
    }

    /**
     * Executes the step synchronously with retry support using default duration and event names.
     *
     * @param stepName    name of the step.
     * @param payload     payload to be passed to a step.
     * @param action      action to be executed.
     * @param retryPolicy retry policy for the step.
     * @return successful result of processing.
     */
    public Map<String, Object> awaitExecute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> payload,
            @Nonnull PayloadProcessor action,
            @Nonnull RetryPolicy retryPolicy) {
        return this.awaitExecute(stepName, payload, action, defaultTimeout, defaults(), retryPolicy);
    }


    /**
     * Synchronous (blocking) execute a step.
     *
     * @param stepName            name of the step.
     * @param payload             payload to be passed to a step.
     * @param returnType          type of the result.
     * @param action              action to be executed.
     * @param eventNameCustomizer event name customizer.
     * @param <T>                 type of result.
     * @return result of the step.
     */
    public <T> T awaitExecute(String stepName, Map<String, Object> payload, Class<T> returnType,
                              Function<Map<String, Object>, T> action, EventNameCustomizer eventNameCustomizer) {
        var stepSpecificName = "__" + stepName;
        var command = PrimitiveCommands.blockingLocalExecute(stepName, payload,
                                                             (c, p) -> {
                                                                 var stepResult = action.apply(p);
                                                                 if (stepResult != null) {
                                                                     return Map.of(stepSpecificName,
                                                                                   stepResult);
                                                                 } else {
                                                                     return Map.of();
                                                                 }
                                                             },
                                                             defaultTimeout,
                                                             new TypeReference<Map<String, Object>>() {
                                                             },
                                                             eventNameCustomizer,
                                                             super.processingContext()
        );
        //noinspection unchecked
        return (T) execute(command).get(stepSpecificName);
    }

    /**
     * Synchronous (blocking) execute a step (without a result).
     *
     * @param stepName step name.
     * @param payload  payload to pass
     * @param action   action to execute.
     */
    public void awaitExecute(String stepName, Map<String, Object> payload, Consumer<Map<String, Object>> action) {
        this.awaitExecute(stepName, payload, (pc, p) -> {
            action.accept(p);
            return Map.of();
        });
    }

    /**
     * Synchronous (blocking) execute a step (with a result).
     *
     * @param stepName   name of the step.
     * @param payload    payload to pass to the step.
     * @param returnType return type of the result.
     * @param action     action to execute.
     * @param <T>        type of the result.
     * @return result of the step.
     */
    public <T> T awaitExecute(String stepName, Map<String, Object> payload, Class<T> returnType,
                              Function<Map<String, Object>, T> action) {
        return this.awaitExecute(stepName, payload, returnType, action, defaults());
    }

    /**
     * Synchronous (blocking) execute a step (with a result).
     *
     * @param stepName   name of the step.
     * @param returnType return type of the result.
     * @param action     action to execute.
     * @param <T>        type of the result.
     * @return result of the step.
     */
    public <T> T awaitExecute(String stepName, Class<T> returnType, Function<Map<String, Object>, T> action) {
        return this.awaitExecute(stepName, Map.of(), returnType, action);
    }

    /**
     * Synchronous (blocking) execute a step (without a result).
     *
     * @param stepName   name of the step.
     * @param returnType return type of the result.
     * @param action     action to execute.
     * @param <T>        type of the result.
     * @return result of the step.
     */
    public <T> T awaitExecute(String stepName, Class<T> returnType, Supplier<T> action) {
        return this.awaitExecute(stepName, Map.of(), returnType, (p) -> action.get());
    }


    /**
     * Terminates the entire workflow with an error, publishing a failure event and cancelling all running steps.
     *
     * @param cause the exception that caused the failure
     * @throws WorkflowFailedException always, after the failure event is published
     */
    public void fail(Throwable cause) {
        terminate(TerminateCommand.fail(cause, defaults()));
    }

    /**
     * Terminates the entire workflow with an error, publishing a failure event and cancelling all running steps.
     *
     * @param cause               the exception that caused the failure
     * @param eventNameCustomizer customizer for the published failure event name
     * @throws WorkflowFailedException always, after the failure event is published
     */
    public void fail(Throwable cause, EventNameCustomizer eventNameCustomizer) {
        terminate(TerminateCommand.fail(cause, eventNameCustomizer));
    }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and cancelling all running steps.
     *
     * @throws WorkflowCancelledException always, after the cancellation event is published
     */
    public void cancel() {
        terminate(TerminateCommand.cancel(defaults()));
    }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and cancelling all running steps.
     *
     * @param eventNameCustomizer customizer for the published cancellation event name
     * @throws WorkflowCancelledException always, after the cancellation event is published
     */
    public void cancel(EventNameCustomizer eventNameCustomizer) {
        terminate(TerminateCommand.cancel(eventNameCustomizer));
    }

    /**
     * Cancels the entire workflow gracefully with a human-readable reason, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @param reason descriptive reason for the cancellation
     * @throws WorkflowCancelledException always, after the cancellation event is published
     */
    public void cancel(String reason) {
        terminate(TerminateCommand.cancel(new WorkflowCancelledException(reason),
                                          defaults()));
    }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and cancelling all running steps.
     *
     * @param cause the exception that triggered the cancellation
     * @throws WorkflowCancelledException always, after the cancellation event is published
     */
    public void cancel(Throwable cause) {
        terminate(TerminateCommand.cancel(cause, defaults()));
    }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and cancelling all running steps.
     *
     * @param cause               the exception that triggered the cancellation
     * @param eventNameCustomizer customizer for the published cancellation event name
     * @throws WorkflowCancelledException always, after the cancellation event is published
     */
    public void cancel(Throwable cause, EventNameCustomizer eventNameCustomizer) {
        terminate(TerminateCommand.cancel(cause, eventNameCustomizer));
    }

    /**
     * Cancels a single running step by name without terminating the workflow. The step's future is completed
     * exceptionally with a {@link StepCancellationException}.
     *
     * @param stepName the name of the step to cancel
     */
    public void cancelStep(String stepName) {
        terminate(TerminateCommand.cancelledStep(stepName, null, defaults()));
    }

    /**
     * Cancels a single running step by name without terminating the workflow. The step's future is completed
     * exceptionally with the given cause, wrapped in a {@link StepCancellationException} if it isn't one already.
     *
     * @param stepName the name of the step to cancel
     * @param cause    the exception that caused the step cancellation
     */
    public void cancelStep(String stepName, Throwable cause) {
        terminate(TerminateCommand.cancelledStep(stepName, cause, defaults()));
    }

    /**
     * Cancels a single running step by name without terminating the workflow. The step's future is completed
     * exceptionally with a {@link StepCancellationException} carrying the given reason.
     *
     * @param stepName the name of the step to cancel
     * @param reason   descriptive reason for the step cancellation
     */
    public void cancelStep(String stepName, String reason) {
        terminate(TerminateCommand.cancelledStep(stepName,
                                                 new StepCancellationException(reason),
                                                 defaults()));
    }

    /**
     * Sets the payload of the workflow instance, converting the object into a <code>Map<String, Object></code> and
     * replacing all payload keys if there are duplicates.
     *
     * @param stepName the name of the payload modification payload step.
     * @param object   the object to be converted into a payload.
     */
    public void setPayload(@Nonnull String stepName, @Nonnull Object object) {
        setPayload(stepName, payload(this, object));
    }

    /**
     * Sets the payload of the workflow instance and replacing all payload keys if there are duplicates.
     *
     * @param stepName the name of the payload modification payload step.
     * @param payload  the object to be set.
     */
    public void setPayload(@Nonnull String stepName, @Nonnull Payload payload) {
        setPayload(stepName, payload, defaults());
    }

    /**
     * Sets the payload of the workflow instance and replacing all payload keys if there are duplicates.
     *
     * @param stepName            the name of the payload modification payload step.
     * @param payload             the object to be set.
     * @param eventNameCustomizer the customizer for event names.
     */
    public void setPayload(@Nonnull String stepName, @Nonnull Payload payload,
                           EventNameCustomizer eventNameCustomizer) {
        modifyPayload(stepName,
                      workflowPayload ->
                              super.processingContext().component(PayloadReducerRegistry.class)
                                   .get(NAME_COMBINE_GLOBAL_AND_LOCAL).orElseThrow()
                                   .apply(
                                           workflowPayload, payload.getValues()
                                   ),
                      eventNameCustomizer);
    }

    /**
     * Sets default timeout for all steps.
     *
     * @param defaultTimeout default timeout for all steps, if not specified explicitly
     */
    public void setDefaultTimeout(@Nonnull Duration defaultTimeout) {
        this.defaultTimeout = Objects.requireNonNull(defaultTimeout, "Default timeout must not be null.");
    }
}
