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

package io.axoniq.framework.axonserver.connector.api;


import io.axoniq.axonserver.grpc.ErrorMessage;
import org.axonframework.common.AxonException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Generic exception indicating an error related to AxonServer.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class AxonServerException extends AxonException {

    private final String code;
    private final String source;
    private final List<String> details;

    /**
     * Initializes the exception using the given {@code code} and {@code errorMessage}.
     *
     * @param code         The code of the error received from the Axon Server
     * @param errorMessage The grpc error message describing the error
     */
    public AxonServerException(String code, ErrorMessage errorMessage) {
        this(errorMessage.getMessage(), code, errorMessage.getLocation(), errorMessage.getDetailsList());
    }

    /**
     * Initializes the exception using the given {@code code} and {@code message}.
     *
     * @param code    The code of the error received from the Axon Server
     * @param message The message describing the exception
     */
    public AxonServerException(String code, String message) {
        this(message, code, null, Collections.emptyList());
    }

    /**
     * Initializes the exception using the given {@code message}, {@code code}, {@code source} and {@code details}.
     *
     * @param message The message describing the exception
     * @param code    The code of the error received from the Axon Server
     * @param source  The location that originally reported the error
     * @param details A {@link List} of {@link String}s, each describing a single "cause"
     */
    public AxonServerException(String message,
                               String code,
                               String source,
                               List<String> details) {
        super(message);
        this.code = code;
        this.source = source;
        this.details = details;
    }

    /**
     * Initializes the exception using the given {@code message}, {@code code}, and {@code cause}.
     *
     * @param message The message describing the exception
     * @param code    The code of the error received from the Axon Server
     * @param cause   The underlying cause of the exception
     */
    public AxonServerException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.details = Collections.emptyList();
        this.source = null;
    }

    public String code() {
        return code;
    }

    public String source() {
        return source;
    }

    public Collection<String> details() {
        return details;
    }
}
