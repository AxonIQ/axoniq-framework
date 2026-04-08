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
 * Interface describing the properties of serialized Domain Event Messages. Event Store implementations should have
 * their storage entries implement this interface.
 *
 * @param <T> The content type of the serialized data
 * @author Allard Buijze
 * @since 2.0
 * @deprecated Will be removed entirely in favor of the {@link EventMessage}.
 */
@Deprecated(since = "5.0.0", forRemoval = true)
public interface DomainEventData<T> extends EventData<T> {

    /**
     * Returns the type identifier of the aggregate.
     *
     * @return the type identifier of the aggregate.
     */
    String getType();

    /**
     * Returns the Identifier of the Aggregate to which the Event was applied.
     *
     * @return the Identifier of the Aggregate to which the Event was applied
     */
    String getAggregateIdentifier();

    /**
     * Returns the sequence number of the event in the aggregate.
     *
     * @return the sequence number of the event in the aggregate
     */
    long getSequenceNumber();
}
