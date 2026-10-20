/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
