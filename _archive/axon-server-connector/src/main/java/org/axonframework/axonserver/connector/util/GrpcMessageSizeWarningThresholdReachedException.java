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

package org.axonframework.axonserver.connector.util;

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
