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

package org.axonframework.modelling.saga.metamodel;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.modelling.saga.AssociationValue;

import java.util.List;
import java.util.Optional;
import java.util.Set;

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

    /**
     * Returns the {@link QualifiedName QualifiedNames} of all {@link EventMessage events} supported by the Saga
     * described by this model, that is, the events for which the Saga declares a
     * {@link org.axonframework.modelling.saga.SagaEventHandler @SagaEventHandler} method.
     *
     * @return the {@link QualifiedName QualifiedNames} of all {@link EventMessage events} supported by the Saga
     * described by this model
     */
    Set<QualifiedName> supportedEvents();

    /**
     * Returns the payload type of the handler that is declared for events with the given {@code eventName}.
     * <p>
     * Sagas resolve handlers and association values from the payload's runtime type. An event read from an event store
     * carries a serialized payload, which has to be converted to this type first.
     *
     * @param eventName the qualified name of the event
     * @return the handler's payload type, or empty if no handler is declared for the given name
     */
    Optional<Class<?>> payloadTypeFor(QualifiedName eventName);
}
