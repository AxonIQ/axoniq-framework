/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.WrappedMessageHandlingMember;

/**
 * A data holder containing information of {@link SagaEventHandler} annotated methods.
 *
 * @author Allard Buijze
 * @author Sofia Guy Ang
 * @since 2.0
 */
public class SagaMethodMessageHandlingMember<T> extends WrappedMessageHandlingMember<T> {

    private final SagaCreationPolicy creationPolicy;
    private final String associationKey;
    private final String associationPropertyName;
    private final AssociationResolver associationResolver;

    /**
     * Creates a SagaMethodMessageHandler.
     *
     * @param creationPolicy          The creation policy for the handlerMethod
     * @param delegate                The message handler for the event
     * @param associationKey          The association key configured for this handler
     * @param associationPropertyName The association property name to look up in the message
     * @param associationResolver     The association resolver configured for this handler
     */
    public SagaMethodMessageHandlingMember(MessageHandlingMember<T> delegate,
                                           SagaCreationPolicy creationPolicy,
                                           String associationKey,
                                           String associationPropertyName,
                                           AssociationResolver associationResolver) {
        super(delegate);
        this.creationPolicy = creationPolicy;
        this.associationKey = associationKey;
        this.associationPropertyName = associationPropertyName;
        this.associationResolver = associationResolver;
    }

    /**
     * The AssociationValue to find the saga instance with, or {@code null} if no AssociationValue can be found on the
     * given {@code eventMessage}.
     *
     * @param eventMessage The event message containing the value of the association
     * @return the AssociationValue to find the saga instance with, or {@code null} if none found
     */
    public AssociationValue getAssociationValue(EventMessage eventMessage) {
        if (associationResolver == null) {
            return null;
        }
        Object associationValue = associationResolver.resolve(associationPropertyName, eventMessage, this);
        return new AssociationValue(associationKey, associationValue == null ? null : associationValue.toString());
    }

    /**
     * Returns the creation policy of the inspected method.
     *
     * @return the creation policy of the inspected method
     */
    public SagaCreationPolicy getCreationPolicy() {
        return creationPolicy;
    }
}
