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

import io.axoniq.workflow.dsl.AbstractDSLWorkflowContext;
import io.axoniq.workflow.dsl.Payload;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.PayloadProcessor;
import io.axoniq.workflow.runtime.api.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.EventConditions;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.axoniq.workflow.dsl.Payload.payload;
import static io.axoniq.workflow.runtime.api.PayloadReducer.all;
import static io.axoniq.workflow.runtime.api.PayloadReducer.local;
import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.defaults;

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

    private Duration defaultTimeout;

    public SimpleWorkflowContext(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull ProcessingContext processingContext,
            @Nonnull EventNameCustomizer parentCustomizer
    ) {
        super(workflowId, payload, processingContext, parentCustomizer);
        defaultTimeout = Duration.ofSeconds(5);
    }

    public <T> T awaitEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout) {
        return waitFor(PrimitiveCommands.blockingWait(
                stepName,
                EventConditions.fromQualifiedName(
                        super.processingContext().component(MessageTypeResolver.class).resolve(eventType).orElseThrow()
                             .qualifiedName(),
                        e -> predicate.test(e.payloadAs(eventType))
                ),
                timeout,
                TypeReference.fromType(eventType),
                super.processingContext().component(Converter.class),
                defaults()
        ));
    }

    public <T> T awaitEvent(String stepName, Class<T> eventType, Duration timeout) {
        return this.awaitEvent(stepName, eventType, e -> true, timeout);
    }

    /**
     * Blocks the execution until the timeout occurs (synchronous call).
     *
     * @param stepName name of the step.
     * @param timeout  timeout to wait.
     */
    public void sleep(String stepName, Duration timeout) {
        var result = waitFor(stepName, EventConditions.never(), timeout, defaults());
        if (result.failure() && result.error().isPresent()) {
            throw result.error().get();
        }
    }

    /**
     * Execute a step asynchronously.
     * <p>The payload passed will be used as step input and the result will be added to the workflow instance
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
        return execute(stepName, payload, action, local(), all(), duration, eventNameCustomizer);
    }

    /**
     * Execute a step asynchronously using default timeout and event names.
     * <p>The payload passed will be used as step input and the result will be added to the workflow instance
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
        return execute(stepName, payload, action, local(), all(), defaultTimeout, defaults());
    }


    /**
     * Executes the step synchronously.
     * <p>The payload passed will be used as step input and the result will be added to the workflow instance
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
                                                       super.processingContext().component(Converter.class),
                                                       eventNameCustomizer
                )
        );
    }

    /**
     * Executes the step synchronously using default duration and event names.
     * <p>The payload passed will be used as step input and the result will be added to the workflow instance
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
                                                             super.processingContext().component(Converter.class),
                                                             eventNameCustomizer
        );
        //noinspection unchecked
        return (T) execute(command).get(stepSpecificName);
    }

    public void awaitExecute(String stepName, Map<String, Object> payload, Consumer<Map<String, Object>> action) {
        this.awaitExecute(stepName, payload, (pc, p) -> {
            action.accept(p);
            return Map.of();
        });
    }

    public <T> T awaitExecute(String stepName, Map<String, Object> payload, Class<T> returnType,
                              Function<Map<String, Object>, T> action) {
        return this.awaitExecute(stepName, payload, returnType, action, defaults());
    }

    public <T> T awaitExecute(String stepName, Class<T> returnType, Function<Map<String, Object>, T> action) {
        return this.awaitExecute(stepName, Map.of(), returnType, action);
    }

    public <T> T awaitExecute(String stepName, Class<T> returnType, Supplier<T> action) {
        return this.awaitExecute(stepName, Map.of(), returnType, (p) -> action.get());
    }


    public void addPayload(@Nonnull Object object) {
        addPayload(payload(this, object));
    }

    public void addPayload(@Nonnull Payload payload) {
        applyPayloadModification(p -> payload(p).with(payload).getValues());
    }

    public void setDefaultTimeout(@Nonnull Duration defaultTimeout) {
        this.defaultTimeout = Objects.requireNonNull(defaultTimeout, "Default timeout must not be null.");
    }
}
