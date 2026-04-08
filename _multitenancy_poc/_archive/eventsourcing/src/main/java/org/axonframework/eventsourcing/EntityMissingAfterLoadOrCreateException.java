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

package org.axonframework.eventsourcing;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Exception thrown by the {@link EventSourcedEntityFactory} when the entity returned by
 * {@link EventSourcedEntityFactory#create(Object, EventMessage, ProcessingContext)} is {@code null} when calling it with a {@code null}
 * {@code firstEventMessage} during the {@link EventSourcingRepository#loadOrCreate(Object, ProcessingContext)}.
 * <p>
 * This indicates that the factory is incapable of creating an entity without an event message, as is the case with
 * entities that only have a constructor that takes an event message or its payload. If this is the case, the
 * {@link EventSourcingRepository#load(Object, ProcessingContext)} should be used instead, which will leave the entity
 * as {@code null} if no events are found.
 * <p>
 * If the entity has a constructor that takes no arguments or an identifier, the factory should be adjusted to return a
 * non-null entity when no event message is present.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class EntityMissingAfterLoadOrCreateException extends RuntimeException {

    /**
     * Constructs the exception with the given {@code identifier}.
     *
     * @param identifier The identifier of the entity that was attempted to be loaded or created.
     */
    public EntityMissingAfterLoadOrCreateException(Object identifier) {
        super(("The EventSourcedEntityFactory returned a null entity while loadOrCreate was called for identifier: [%s]. "
                + "Adjust your EventSourcedEntityFactory to return a non-null entity when no event message is present.").formatted(
                identifier));
    }
}
