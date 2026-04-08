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

package org.axonframework.eventsourcing.eventstore;

import org.axonframework.common.Assert;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.Set;
import java.util.function.Function;

/**
 * Implementation of the {@link TaggedEventMessage} allowing a generic {@link EventMessage} of type {@code E}.
 *
 * @param event The {@link EventMessage} paired with the given {@code tags}.
 * @param tags  The {@link Set} of {@link Tag Tags} relating to the given {@code event}.
 * @param <E>   The type of {@link EventMessage} carried by this {@link TaggedEventMessage} implementation.
 * @author Steven van Beelen
 * @since 5.0.0
 */
public record GenericTaggedEventMessage<E extends EventMessage>(
        E event,
        Set<Tag> tags
) implements TaggedEventMessage<E> {

    /**
     * Compact constructing asserting that the given {@code event} and {@code tags} are not {@code null}.
     */
    public GenericTaggedEventMessage {
        Assert.notNull(event, () -> "The given event must not be null");
        Assert.notNull(tags, () -> "The given tags collection must not be null");
    }

    @Override
    public TaggedEventMessage<E> updateTags(Function<Set<Tag>, Set<Tag>> updater) {
        return new GenericTaggedEventMessage<>(this.event, updater.apply(this.tags));
    }
}
