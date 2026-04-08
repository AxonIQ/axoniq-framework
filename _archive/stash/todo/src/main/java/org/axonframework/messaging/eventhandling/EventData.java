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

import org.axonframework.conversion.SerializedObject;

import java.time.Instant;

/**
 * Interface describing the properties of serialized Event Messages. Event Storage Engine implementations should have
 * their storage entries implement this interface.
 *
 * @param <T> The content type of the serialized data
 * @author Rene de Waele
 * @deprecated Will be removed entirely in favor of the {@link EventMessage}.
 */
@Deprecated(since = "5.0.0", forRemoval = true)
public interface EventData<T> {

    /**
     * Returns the identifier of the serialized event.
     *
     * @return the identifier of the serialized event
     */
    String getEventIdentifier();

    /**
     * Returns the timestamp at which the event was first created.
     *
     * @return the timestamp at which the event was first created
     */
    Instant getTimestamp();

    /**
     * Returns the serialized data of the Metadata of the serialized Event.
     *
     * @return the serialized data of the Metadata of the serialized Event
     */
    SerializedObject<T> getMetadata();

    /**
     * Returns the serialized data of the Event Message's payload.
     *
     * @return the serialized data of the Event Message's payload
     */
    SerializedObject<T> getPayload();

}
