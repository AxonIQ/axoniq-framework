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

package org.axonframework.messaging.eventhandling;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import org.axonframework.conversion.Serializer;

/**
 * Abstract base class of a serialized domain event. Fields in this class contain JPA annotation that direct JPA event
 * storage engines how to store event entries.
 *
 * @author Rene de Waele
 * @deprecated Will be removed entirely in favor of the {@link EventMessage}.
 */
@Deprecated(since = "5.0.0", forRemoval = true)
@MappedSuperclass
public abstract class AbstractSequencedDomainEventEntry<T> extends AbstractDomainEventEntry<T> implements DomainEventData<T> {

    @Id
    @GeneratedValue
    @SuppressWarnings("unused")
    private long globalIndex;

    /**
     * Construct a new default domain event entry from a published domain event message to enable storing the event or
     * sending it to a remote location. The event payload and metadata will be serialized to a byte array.
     * <p>
     * The given {@code serializer} will be used to serialize the payload and metadata in the given {@code
     * eventMessage}. The type of the serialized data will be the same as the given {@code contentType}.
     *
     * @param eventMessage The event message to convert to a serialized event entry
     * @param serializer   The serializer to convert the event
     * @param contentType  The data type of the payload and metadata after conversion
     */
    public AbstractSequencedDomainEventEntry(DomainEventMessage eventMessage, Serializer serializer,
                                             Class<T> contentType) {
        super(eventMessage, serializer, contentType);
    }

    /**
     * Default constructor required by JPA
     */
    protected AbstractSequencedDomainEventEntry() {
    }
}
