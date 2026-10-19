/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.util;

/**
 * Exception created to log a warning when a message size exceeds a certain threshold.
 * This means that the application has tried to send a message to Axon Server that is almost too large.
 * This can be a command, query, response, or any other message.
 * <p>
 * This exception is never thrown, but only used to log a warning that includes the stack trace for better analysis.
 *
 * @author Mitchell Herrijgers
 * @see GrpcMessageSizeInterceptor
 * @since 4.11.0
 */
public class GrpcMessageSizeWarningThresholdReachedException extends RuntimeException {
    /**
     * Creates a new instance of the exception indicating that the message size has exceeded the threshold.
     */
    public GrpcMessageSizeWarningThresholdReachedException() {
        super("Message size exceeds the warning threshold. This message will be accepted, but it is recommended to reduce the message size or increase the limits.");
    }
}
