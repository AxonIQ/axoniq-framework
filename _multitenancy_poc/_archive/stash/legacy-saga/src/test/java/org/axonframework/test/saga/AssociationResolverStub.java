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

package org.axonframework.test.saga;

import org.axonframework.messaging.eventhandling.DomainEventMessage;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.modelling.saga.AssociationResolver;
import org.axonframework.modelling.saga.PayloadAssociationResolver;
import org.jspecify.annotations.NonNull;

import static org.junit.jupiter.api.Assertions.*;

public class AssociationResolverStub implements AssociationResolver {

    private final PayloadAssociationResolver defaultResolver = new PayloadAssociationResolver();

    @Override
    public <T> void validate(@NonNull String associationPropertyName, @NonNull MessageHandlingMember<T> handler) {
        defaultResolver.validate(associationPropertyName, handler);
    }

    @Override
    public <T> Object resolve(@NonNull String associationPropertyName, @NonNull EventMessage message,
                              @NonNull MessageHandlingMember<T> handler) {


        if (!DomainEventMessage.class.isAssignableFrom(message.getClass())) {
            fail("message is not assignable from DomainEventMessage");
        }
        return defaultResolver.resolve(associationPropertyName, message, handler);
    }
}
