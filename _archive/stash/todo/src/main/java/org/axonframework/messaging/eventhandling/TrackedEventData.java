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

import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

/**
 * Interface describing the properties of serialized Event Messages containing a {@link TrackingToken}. Event Storage
 * Engine implementations should have their storage entries implement this interface.
 *
 * @param <T> The content type of the serialized data
 * @author Rene de Waele
 * @deprecated Will be removed entirely in favor of the {@link EventMessage}.
 */
@Deprecated(since = "5.0.0", forRemoval = true)
public interface TrackedEventData<T> extends EventData<T> {

    /**
     * Returns the {@link TrackingToken} of the serialized event.
     *
     * @return the tracking token of the serialized event
     */
    TrackingToken trackingToken();

}
