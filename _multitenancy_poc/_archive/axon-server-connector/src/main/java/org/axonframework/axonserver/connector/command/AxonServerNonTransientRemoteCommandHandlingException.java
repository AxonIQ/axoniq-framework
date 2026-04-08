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

package org.axonframework.axonserver.connector.command;

import io.axoniq.axonserver.grpc.ErrorMessage;
import org.axonframework.messaging.core.RemoteExceptionDescription;
import org.axonframework.messaging.core.RemoteNonTransientHandlingException;

/**
 * Exception indicating a non-transient problem that was reported by the remote end of a connection.
 *
 * @author Stefan Andjelkovic
 * @since 4.5
 */
public class AxonServerNonTransientRemoteCommandHandlingException extends RemoteNonTransientHandlingException {

    private static final boolean PERSISTENT = true;
    private final String errorCode;
    private final String server;

    /**
     * Initialize the exception with given {@code errorCode} and {@code errorMessage}.
     *
     * @param errorCode    the code reported by the server
     * @param errorMessage the message describing the exception on the remote end
     */
    public AxonServerNonTransientRemoteCommandHandlingException(String errorCode, ErrorMessage errorMessage) {
        super(new RemoteExceptionDescription(errorMessage.getDetailsList(), PERSISTENT));
        this.errorCode = errorCode;
        this.server = errorMessage.getLocation();
    }

    /**
     * Returns the name of the server that reported the error.
     *
     * @return the name of the server that reported the error
     */
    public String getServer() {
        return server;
    }

    /**
     * Returns the error code as reported by the server.
     *
     * @return the error code as reported by the server
     */
    public String getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return "AxonServerNonTransientRemoteCommandHandlingException{" +
                "message=" + getMessage() +
                ", errorCode='" + errorCode + '\'' +
                ", server='" + server + '\'' +
                '}';
    }
}
