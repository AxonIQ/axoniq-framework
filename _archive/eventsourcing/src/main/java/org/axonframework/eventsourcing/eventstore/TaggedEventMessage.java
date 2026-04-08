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

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.Set;
import java.util.function.Function;

/**
 * A wrapper of an {@link EventMessage} and its {@link Tag Tags}.
 * <p>
 * {@code Tags} typically refer to the name and value of the identifiers of the model(s) that led to the publication of
 * the contained {@code event}.
 *
 * @param <E> The type of {@link EventMessage} provided by this interface.
 * @author Allard Buijze
 * @author Michal Negacz
 * @author Milan Savić
 * @author Marco Amann
 * @author Sara Pellegrini
 * @author Steven van Beelen
 * @since 5.0.0
 */
public interface TaggedEventMessage<E extends EventMessage> {

    /**
     * Return the {@link EventMessage} of generic type {@code E} carried by this {@code TaggedEventMessage}.
     *
     * @return {@link EventMessage} of generic type {@code E} carried by this {@code TaggedEventMessage}.
     */
    E event();

    /**
     * Return the {@link Set} of {@link Tag Tags} of this {@code TaggedEventMessage}.
     *
     * @return The {@link Set} of {@link Tag Tags} of this {@code TaggedEventMessage}.
     */
    Set<Tag> tags();

    /**
     * Construct a new {@code TaggedEventMessage} using the given {@code updater} to adjust the {@link #tags()} of the
     * new {@code TaggedEventMessage}.
     *
     * @param updater The {@link Function} returning a new {@link Set} of {@link Tag Tags} based on the existing
     *                {@link #tags()}.
     * @return A new {@code TaggedEventMessage} using the given {@code updater} to adjust the {@link #tags()} of the new
     * {@code TaggedEventMessage}.
     */
    TaggedEventMessage<E> updateTags(Function<Set<Tag>, Set<Tag>> updater);
}
