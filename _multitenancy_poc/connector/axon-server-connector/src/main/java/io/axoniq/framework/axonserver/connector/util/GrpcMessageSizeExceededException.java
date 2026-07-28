/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.axonserver.connector.util;

/**
 * Exception thrown when a message exceeds the maximum allowed size. This means that the application has tried
 * to send a message to Axon Server that is too large. This can be a command, query, response, or any other message.
 *
 * @author Mitchell Herrijgers
 * @see GrpcMessageSizeInterceptor
 * @since 4.11.0
 */
public class GrpcMessageSizeExceededException extends RuntimeException {

    /**
     * Creates a new exception that indicates the gRPC message size has been exceeded with the given message.
     * @param message the message to include in the exception
     */
    public GrpcMessageSizeExceededException(String message) {
        super(message);
    }
}
