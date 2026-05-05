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
package io.axoniq.workflow.runtime.association;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Describes association between an event and the workflow instance.
 *
 * @param associationValueRetriever retrieves the value from the event.
 * @param operator                  comparison operator.
 * @param associationValueSupplier  value supplier used for comparison.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public record AssociationValue(
        @Nonnull ValueRetriever associationValueRetriever,
        @Nonnull ValueComparisonOperator operator,
        @Nonnull Supplier<Object> associationValueSupplier
) {

    /**
     * Returns a predicate on a message with its processing context.
     *
     * @return predicate to be applied on the message and processing context using the association value retriever and
     * comparing the value using the specified operator.
     */
    public BiPredicate<EventMessage, ProcessingContext> asEventMessagePredicate() {
        return (eventMessage, pc) ->
                operator.apply(associationValueSupplier.get(),
                               associationValueRetriever.apply(eventMessage, pc));
    }
}
