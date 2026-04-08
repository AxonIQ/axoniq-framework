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

package org.axonframework.modelling.entity.annotation;

/**
 * Exception indicating that a child entity indicated a routing key that is not known on the incoming message. As such,
 * a child entity could not be resolved to handle the message.
 * <p>
 * This issue can be resolved by ensuring the routing key specified on the child entity matches the routing key
 * specified on the incoming message. Ensure that the {@link EntityMember#routingKey} points to a valid member of the
 * message.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class UnknownRoutingKeyException extends RuntimeException {

    /**
     * Creates a new exception with the given {@code message}.
     *
     * @param message The message describing the cause of the exception.
     */
    public UnknownRoutingKeyException(String message) {
        super(message);
    }
}
