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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.modelling.saga.metamodel;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.modelling.saga.AssociationValue;

import java.util.List;
import java.util.Optional;

/**
 * Interface of a model that describes a Saga of type {@code T}. Use the SagaModel to obtain associations and
 * event handlers for the Saga.
 *
 * @param <T> The saga type
 */
public interface SagaModel<T> {

    /**
     * Returns the {@link AssociationValue} used to find sagas of type {@code T} that can handle the given
     * {@code eventMessage}. If the saga type does not handle events of this type an empty Optional is returned.
     *
     * @param eventMessage The event to find the association value for.
     * @param context The {@link ProcessingContext} in which the event is being processed.
     * @return Optional of the AssociationValue for the event, or an empty Optional if the saga doesn't handle the event
     */
    Optional<AssociationValue> resolveAssociation(EventMessage eventMessage, ProcessingContext context);

    /**
     * Returns a {@link List} of {@link MessageHandlingMember} that can handle the given event.
     *
     * @param event   The {@link EventMessage} to be handled.
     * @param context The {@link ProcessingContext} in which the event is being processed.
     * @return Event message handlers for the given {@code event}.
     */
    List<MessageHandlingMember<? super T>> findHandlerMethods(EventMessage event, ProcessingContext context);

    /**
     * Indicates whether the Saga described by this model has a handler for the given {@code eventMessage}
     *
     * @param eventMessage The message to check the availability of a handler for.
     * @param context The {@link ProcessingContext} in which the event is being processed.
     * @return {@code true} if there the Saga has a handler for this message, otherwise {@code false}.
     */
    default boolean hasHandlerMethod(EventMessage eventMessage, ProcessingContext context) {
        return !findHandlerMethods(eventMessage, context).isEmpty();
    }

    /**
     * Returns the factory that created this model.
     *
     * @return The factory that made this model
     */
    SagaMetaModelFactory modelFactory();
}
