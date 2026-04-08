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

package io.axoniq.framework.axonserver.connector.api.query;

import io.axoniq.axonserver.grpc.ErrorMessage;
import org.axonframework.messaging.core.RemoteExceptionDescription;
import org.axonframework.messaging.core.RemoteHandlingException;

import java.io.Serial;

/**
 * An AxonServer Exception which is thrown on a Query Handling exception.
 * <p/>
 * By default, a stack trace is not generated for this exception. However, the stack trace creation can be enforced
 * explicitly via the constructor accepting the {@code writableStackTrace} parameter.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class AxonServerRemoteQueryHandlingException extends RemoteHandlingException {

    @Serial
    private static final long serialVersionUID = -8868624888839585045L;

    private final String errorCode;
    private final String server;

    /**
     * Initialize a Query Handling exception from a remote source.
     *
     * @param errorCode a {@link String} defining the error code of this exception
     * @param message   an {@link ErrorMessage} describing the exception
     */
    public AxonServerRemoteQueryHandlingException(String errorCode, ErrorMessage message) {
        this(errorCode, message, false);
    }

    /**
     * Initialize a Query Handling exception from a remote source.
     *
     * @param errorCode          a {@link String} defining the error code of this exception
     * @param message            an {@link ErrorMessage} describing the exception
     * @param writableStackTrace whether the stack trace should be generated ({@code true}) or not ({@code false}
     */
    public AxonServerRemoteQueryHandlingException(String errorCode, ErrorMessage message, boolean writableStackTrace) {
        super(new RemoteExceptionDescription(message.getDetailsList()), writableStackTrace);
        this.errorCode = errorCode;
        this.server = message.getLocation();
    }

    /**
     * Return a {@link String} defining the error code.
     *
     * @return a {@link String} defining the error code
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Return a {@link String} defining the location where the error originated.
     *
     * @return a {@link String} defining the location where the error originated
     */
    public String getServer() {
        return server;
    }

    @Override
    public String toString() {
        return "AxonServerRemoteQueryHandlingException{" +
                "message=" + getMessage() +
                ", errorCode='" + errorCode + '\'' +
                ", location='" + server + '\'' +
                '}';
    }
}