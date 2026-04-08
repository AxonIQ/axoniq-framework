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

package org.axonframework.messaging.eventhandling.annotation;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;

/**
 * Interface indicating that a {@link MessageHandlingMember} is capable of handling specific event messages.
 *
 * @param <T> The type of entity to which the message handler will delegate the actual handling of the message.
 * @author Mateusz Nowak
 * @since 5.0.0
 */
@Internal
public interface EventHandlingMember<T> extends MessageHandlingMember<T> {

    /**
     * Returns the name of the event that can be handled.
     * <p>
     * Might be an empty {@link String} when undefined by this handling member, in which case components gathering
     * {@code EventHandlingMembers} should fall back to other mechanisms to define the name of a handling member.
     *
     * @return The name of the event that can be handled.
     */
    String eventName();
}