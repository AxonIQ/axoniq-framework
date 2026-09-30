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

package org.axonframework.modelling.saga;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;

/**
 * Used to derive the value of an association property by looking it up the event message's {@link
 * Metadata}.
 *
 * @author Sofia Guy Ang
 */
public class MetadataAssociationResolver implements AssociationResolver {

    /**
     * Does nothing because we can only check for existence of property in the metadata during event handling.
     */
    @Override
    public <T> void validate(String associationPropertyName, MessageHandlingMember<T> handler) {
        // Do nothing
    }

    /**
     * Finds the association property value by looking up the association property name in the event message's {@link
     * Metadata}.
     */
    @Override
    public <T> Object resolve(String associationPropertyName, EventMessage message,
                              MessageHandlingMember<T> handler) {
        return message.metadata().get(associationPropertyName);
    }
}
