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


/**
 * Used to derive the value of an association property as designated by the association property name.
 *
 * @author Sofia Guy Ang
 */
public interface AssociationResolver {

    /**
     * Validates that the associationPropertyName supplied is compatible with the handler.
     */
    <T> void validate(String associationPropertyName, MessageHandlingMember<T> handler);

    /**
     * Resolves the associationPropertyName as a value.
     */
    <T> Object resolve(String associationPropertyName, EventMessage message,
                       MessageHandlingMember<T> handler);
}
