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

/**
 * Generic implementation of a serialized domain event entry. This implementation can be used by Event Storage Engine
 * implementations to reconstruct an event or snapshot from the underlying storage for example.
 *
 * @author Rene de Waele
 * @deprecated Will be removed entirely in favor of the {@link EventMessage}.
 */
@Deprecated(since = "5.0.0", forRemoval = true)
public class GenericDomainEventEntry<T> extends AbstractDomainEventEntry<T> {

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
    public GenericDomainEventEntry(String type, String aggregateIdentifier, long sequenceNumber, String eventIdentifier,
                                   Object timestamp, String payloadType, String payloadRevision, T payload,
                                   T metadata) {
        super(type, aggregateIdentifier, sequenceNumber, eventIdentifier, timestamp, payloadType, payloadRevision,
              payload, metadata);
    }
}
