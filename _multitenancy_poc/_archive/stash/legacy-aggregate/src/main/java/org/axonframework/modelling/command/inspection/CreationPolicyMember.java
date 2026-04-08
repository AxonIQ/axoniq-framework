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

package org.axonframework.modelling.command.inspection;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.modelling.command.AggregateCreationPolicy;

/**
 * Interface specifying a message handler containing a creation policy definition.
 *
 * @param <T> The type of entity to which the message handler will delegate the actual handling of the message.
 * @author Marc Gathier
 * @since 4.3.0
 */
@Internal
public interface CreationPolicyMember<T> extends MessageHandlingMember<T> {

    /**
     * Returns the creation policy set on the {@link MessageHandlingMember}.
     *
     * @return the creation policy set on the handler
     */
    AggregateCreationPolicy creationPolicy();
}
