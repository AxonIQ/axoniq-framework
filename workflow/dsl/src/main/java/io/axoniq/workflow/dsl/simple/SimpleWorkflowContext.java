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
import io.axoniq.workflow.runtime.api.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.WorkflowServices;
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
import static io.axoniq.workflow.runtime.api.PayloadReducer.all;
import static io.axoniq.workflow.runtime.api.PayloadReducer.local;
import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName;


public class SimpleWorkflowContext extends WorkflowInstance
        implements WaitForPrimitive, ExecutePrimitive {

    public SimpleWorkflowContext(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull ProcessingContext processingContext,
            @Nonnull EventNameCustomizer parentCustomizer,
            @Nonnull WorkflowServices workflowServices
    ) {
        super(workflowId, payload, processingContext, parentCustomizer, workflowServices);
    }

    public <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout) {
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

    public <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout) {
        return this.waitForEvent(stepName, eventType, e -> true, timeout);
    }

    public <T> T waitForEvent(String stepName, Class<T> eventType) {
        return this.waitForEvent(stepName, eventType, Duration.ofSeconds(5));
    }

    public void wait(String stepName, Duration timeout) {
        var result = waitFor(stepName, new QualifiedName(Void.class), (e) -> false, timeout, eventName());
        if (result.isFailure() && result.error().isPresent()) {
            throw result.error().get();
        }
    }

    public WorkflowStepResult executeWithResult(String stepName, Map<String, Object> payload, PayloadProcessor action,
                                                Duration duration) {
        return execute(stepName, payload, action, local(), all(), duration, eventName());
    }


    public Map<String, Object> execute(String stepName, Map<String, Object> payload, PayloadProcessor action,
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

    public <T> T execute(String stepName, Map<String, Object> payload, Class<T> returnType,
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

    public Map<String, Object> execute(String stepName, Map<String, Object> payload, PayloadProcessor action) {
        return this.execute(stepName, payload, action, Duration.ofSeconds(5));
    }

    public <T> T execute(String stepName, Map<String, Object> payload, Class<T> returnType,
                         Function<Map<String, Object>, T> action) {
        return this.execute(stepName, payload, returnType, action, eventName());
    }

    public <T> T execute(String stepName, Class<T> returnType, Supplier<T> action) {
        return this.execute(stepName, Map.of(), returnType, (p) -> action.get());
    }

    public void execute(String stepName, Runnable action) {
        this.execute(stepName, Void.class, () -> {
            action.run();
            return null;
        });
    }

    public <T> T execute(String stepName, Payload payload, Class<T> returnType, Function<Payload, T> action) {
        return this.execute(stepName, payload.getValues(), returnType, (m) -> action.apply(payload(m)));
    }

    public void addPayload(Object object) {
        addPayload(payload(this, object));
    }

    public void addPayload(Payload payload) {
        applyPayloadModification(p -> payload(p).with(payload).getValues());
    }

    <T> T fromResult(WorkflowStepResult result, Class<T> eventType) {
        if (result.isSuccess() && result.payload().isPresent()) {
            return super.processingContext().component(Converter.class)
                        .convert(result.payload().get(), eventType);
        } else {
            throw result.error().orElseThrow();
        }
    }

    Map<String, Object> fromResult(WorkflowStepResult result) {
        if (result.isSuccess() && result.<Map<String, Object>>payload().isPresent()) {
            return result.<Map<String, Object>>payload().get();
        } else {
            throw result.error().orElseThrow();
        }
    }
}
