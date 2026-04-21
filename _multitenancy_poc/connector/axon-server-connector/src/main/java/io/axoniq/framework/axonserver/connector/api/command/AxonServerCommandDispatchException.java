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

package io.axoniq.framework.axonserver.connector.api.command;

import io.axoniq.axonserver.grpc.ErrorMessage;
import org.axonframework.messaging.commandhandling.CommandDispatchException;

import java.util.Collections;
import java.util.List;

/**
 * Exception thrown if there is a problem dispatching a command on Axon Server.
 *
 * @author Milan Savic
 * @since 4.0
 */
public class AxonServerCommandDispatchException extends CommandDispatchException {

    private static final long serialVersionUID = -6427074119385898085L;

    private final String errorCode;
    private final String server;
    private final List<String> exceptionDescriptions;

    /**
     * Initialize the exception with given {@code errorCode} and {@code errorMessage}.
     *
     * @param errorCode    the code reported by the server
     * @param errorMessage the message describing the exception on the remote end
     */
    public AxonServerCommandDispatchException(String errorCode, ErrorMessage errorMessage) {
        super(errorMessage.getMessage());
        this.errorCode = errorCode;
        this.server = errorMessage.getLocation();
        this.exceptionDescriptions = errorMessage.getDetailsList();
    }

    public AxonServerCommandDispatchException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.server = null;
        this.exceptionDescriptions = Collections.emptyList();
    }

    /**
     * Returns the error code as reported by the server.
     *
     * @return the error code as reported by the server
     */
    public String getErrorCode() {
        return errorCode;
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
     * Returns a {@link List} of {@link String}s describing the remote exception.
     *
     * @return a {@link List} of {@link String}s describing the remote exception
     */
    public List<String> getExceptionDescriptions() {
        return exceptionDescriptions;
    }

    @Override
    public String toString() {
        return "AxonServerCommandDispatchException{" +
                "message=" + getMessage() +
                ", errorCode='" + errorCode + '\'' +
                ", server='" + server + '\'' +
                '}';
    }
}