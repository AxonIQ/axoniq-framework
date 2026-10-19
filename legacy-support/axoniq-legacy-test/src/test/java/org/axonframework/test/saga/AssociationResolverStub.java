/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.test.saga;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.modelling.saga.AssociationResolver;
import org.axonframework.modelling.saga.PayloadAssociationResolver;
import org.jspecify.annotations.NonNull;

/**
 * Delegates to the {@link PayloadAssociationResolver}, to prove a custom resolver is consulted at all.
 * <p>
 * Axon Framework 4 also asserted here that the message was a {@code DomainEventMessage}, which is how it checked that
 * an aggregate publisher produced one. Axon Framework 5 has no such message, and a resolver is handed no processing
 * context, so it cannot see the aggregate fields that replaced it. That check moved to
 * {@code SagaTestFixtureGivenWhenTest}, where a Saga handler reads them as parameters.
 */
public class AssociationResolverStub implements AssociationResolver {

    private final PayloadAssociationResolver defaultResolver = new PayloadAssociationResolver();

    @Override
    public <T> void validate(@NonNull String associationPropertyName, @NonNull MessageHandlingMember<T> handler) {
        defaultResolver.validate(associationPropertyName, handler);
    }

    @Override
    public <T> Object resolve(@NonNull String associationPropertyName, @NonNull EventMessage message,
                              @NonNull MessageHandlingMember<T> handler) {
        return defaultResolver.resolve(associationPropertyName, message, handler);
    }
}
