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

package org.axonframework.messaging.core;

/**
 * A contract towards being a distributed message bus implementation. The generic {@code T} defines the type of message
 * bus which is distributed.
 *
 * @param <MessageBus> the message bus which is distributed
 * @author Steven van Beelen
 * @since 4.2.2
 */
public interface Distributed<MessageBus> {

    /**
     * Return the message bus of type {@code MessageBus} which is regarded as the local segment for this implementation.
     * Would return the message bus used to dispatch and handle messages in a local environment to bridge the gap in a
     * distributed set up.
     *
     * @return a {@code MessageBus} which is the local segment for this distributed message bus implementation
     */
    MessageBus localSegment();
}
