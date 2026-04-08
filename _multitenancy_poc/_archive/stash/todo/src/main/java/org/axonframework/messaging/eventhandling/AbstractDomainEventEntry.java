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

import jakarta.persistence.Basic;
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
public abstract class AbstractDomainEventEntry<T> extends AbstractEventEntry<T> implements DomainEventData<T> {

    @Basic
    private String type;
    @Basic(optional = false)
    private String aggregateIdentifier;
    @Basic(optional = false)
    private long sequenceNumber;

    /**
     * Construct a new event entry from a published domain event message to enable storing the event or sending it to a
     * remote location.
     * <p>
     * The given {@code serializer} will be used to serialize the payload and metadata in the given {@code eventMessage}.
     * The type of the serialized data will be the same as the given {@code contentType}.
     *
     * @param eventMessage The event message to convert to a serialized event entry
     * @param serializer   The serializer to convert the event
     * @param contentType  The data type of the payload and metadata after conversion
     */
    public AbstractDomainEventEntry(DomainEventMessage eventMessage, Serializer serializer, Class<T> contentType) {
        super(eventMessage, serializer, contentType);
        type = eventMessage.getType();
        aggregateIdentifier = eventMessage.getAggregateIdentifier();
        sequenceNumber = eventMessage.getSequenceNumber();
    }

    /**
     * Reconstruct an event entry from a stored object.
     *
     * @param type                The type of aggregate that published this event
     * @param aggregateIdentifier The identifier of the aggregate that published this event
     * @param sequenceNumber      The sequence number of the event in the aggregate
     * @param eventIdentifier     The identifier of the event
     * @param timestamp           The time at which the event was originally created
     * @param payloadType         The fully qualified class name or alias of the event payload
     * @param payloadRevision     The revision of the event payload
     * @param payload             The serialized payload
     * @param metadata            The serialized metadata
     */
    public AbstractDomainEventEntry(String type, String aggregateIdentifier, long sequenceNumber,
                                    String eventIdentifier, Object timestamp, String payloadType,
                                    String payloadRevision, T payload, T metadata) {
        super(eventIdentifier, timestamp, payloadType, payloadRevision, payload, metadata);
        this.type = type;
        this.aggregateIdentifier = aggregateIdentifier;
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * Default constructor required by JPA
     */
    protected AbstractDomainEventEntry() {
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }
}
