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

package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;

/**
 * Holds wait for event conditions for a single workflow instance.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
final class EventWaitConditions implements DescribableComponent {

    private final ConcurrentHashMap<String, EventConditionWithStepNameCustomizer> waitConditions = new ConcurrentHashMap<>();

    /**
     * Internal representation.
     *
     * @param eventCondition       condition to match.
     * @param resultPayloadReducer payload reducer to combine payload delivered by the event (result of the step) with
     *                             the workflow payload.
     * @param eventNameCustomizer  customizer.
     */
    @Internal
    record EventConditionWithStepNameCustomizer(
            @Nonnull EventCondition eventCondition,
            @Nonnull PayloadReducer resultPayloadReducer,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {

    }

    /**
     *
     * Adds a new event wait condition for the specified workflow step.
     *
     * @param stepName             step waiting for event.
     * @param resultPayloadReducer payload reducer to combine payload delivered by the event (result of the step) with
     *                             the workflow payload.
     * @param eventCondition       await condition
     */
    public void add(@Nonnull String stepName,
                    @Nonnull EventCondition eventCondition,
                    @Nonnull PayloadReducer resultPayloadReducer,
                    @Nullable EventNameCustomizer eventNameCustomizer) {
        waitConditions.put(stepName,
                           new EventConditionWithStepNameCustomizer(eventCondition,
                                                                    resultPayloadReducer,
                                                                    eventNameCustomizer
                                                                            != null ? eventNameCustomizer : defaults()));
    }

    /**
     * Removes condition for a given step.
     *
     * @param stepName step name waiting for event.
     */
    public void remove(@Nonnull String stepName) {
        waitConditions.remove(stepName);
    }

    /**
     * Evaluates existing conditions on the provided event message and applies the provided action if the match is
     * found.
     * <p>
     * If the condition is met on the provided message, it will be removed from wait conditions and the message will be
     * passed to the action.
     * </p>
     *
     * @param eventMessage      the event message to evaluate existing conditions against
     * @param processingContext the processing context within which the {@code eventMessage} is being handled
     * @param action            the action executed on an event message, step name and condition, when it is met
     *
     */
    public void evaluateAndApply(@Nonnull EventMessage eventMessage,
                                 @Nonnull ProcessingContext processingContext,
                                 @Nonnull Consumer<Awaited> action) {
        // TODO synchronized ?
        for (var entry : waitConditions.entrySet()) {
            var condition = entry.getValue().eventCondition;
            var stepName = entry.getKey();
            if (eventMessage.type().qualifiedName().equals(condition.qualifiedName())
                    && condition.predicate().test(eventMessage, processingContext)) {
                remove(stepName);
                action.accept(
                        new Awaited(eventMessage,
                                    processingContext,
                                    stepName,
                                    entry.getValue().resultPayloadReducer(),
                                    entry.getValue().eventNameCustomizer())
                );
            }
        }
    }

    /**
     * Expresses the arrival of the event message passed to the
     * {@link #evaluateAndApply(EventMessage, ProcessingContext, Consumer)}.
     *
     * @param eventMessage        event message.
     * @param processingContext   processing context.
     * @param payloadReducer      payload reducer.
     * @param stepName            step name.
     * @param eventNameCustomizer event name customizer.
     */
    public record Awaited(
            @Nonnull EventMessage eventMessage,
            @Nonnull ProcessingContext processingContext,
            @Nonnull String stepName,
            @Nonnull PayloadReducer payloadReducer,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {

    }

    /**
     * Clears all event wait conditions.
     */
    public void clear() {
        this.waitConditions.clear();
    }


    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        var conditions = waitConditions.entrySet().stream()
                                       .map(e -> new EventWaitConditionDescriptor(e.getKey(),
                                                                                  e.getValue().eventCondition))
                                       .toList();
        descriptor.describeProperty("waitConditions", conditions);
    }

    private record EventWaitConditionDescriptor(String stepName,
                                                EventCondition condition)
            implements DescribableComponent {

        @Override
        public void describeTo(@Nonnull ComponentDescriptor descriptor) {
            var description = condition.qualifiedName().toString();
            if (!condition.associations().isEmpty()) {
                description += " where " + String.join(" && ", condition.associations());
            }
            descriptor.describeProperty(stepName, description);
        }
    }
}
