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

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;

import java.util.List;

/**
 * Interface describing en entity that is a child of another entity.
 *
 * @param <T> defining the parent class this {@link ChildEntity} belongs to
 * @author Allard Buijze
 * @since 3.0
 */
public interface ChildEntity<T> {

    /**
     * Publish the given {@code msg} to the appropriate handlers on the given {@code declaringInstance}.
     *
     * @param msg               the message to publish
     * @param declaringInstance the instance of this entity to invoke handlers on
     */
    void publish(EventMessage msg, T declaringInstance);

    /**
     * Returns the command handlers declared in this entity.
     *
     * @return a list of {@link MessageHandlingMember}s that are capable of processing command messages
     */
    List<MessageHandlingMember<? super T>> commandHandlers();
}
