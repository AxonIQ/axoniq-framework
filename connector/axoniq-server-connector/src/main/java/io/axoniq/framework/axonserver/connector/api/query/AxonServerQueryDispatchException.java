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
import io.axoniq.framework.axonserver.connector.shared.ErrorCode;
import org.axonframework.common.AxonException;

import java.io.Serial;
import java.util.Collections;
import java.util.List;

/**
 * An AxonServer Exception which is thrown on a Query Dispatching exception.
 *
 * @author Steven van Beelen
 * @since 4.0
 */
public class AxonServerQueryDispatchException extends AxonException {

    @Serial
    private static final long serialVersionUID = 4810265327922717359L;

    private final String code;
    private final String source;
    private final List<String> details;

    /**
     * Initializes an AxonServer Query Dispatch Exception using the given {@code code} and {@code errorMessage}.
     *
     * @param code         a {@link String} specifying the error code received from the Axon Server
     * @param errorMessage the grpc {@link ErrorMessage} describing the error
     */
    public AxonServerQueryDispatchException(String code, ErrorMessage errorMessage) {
        this(errorMessage.getMessage(), code, errorMessage.getLocation(), errorMessage.getDetailsList());
    }

    /**
     * Initializes an AxonServer Query Dispatch Exception using the given {@code code} and {@code message}.
     *
     * @param code    a {@link String} specifying the error code received from the Axon Server
     * @param message a {@link String} describing the exception message
     */
    public AxonServerQueryDispatchException(String code, String message) {
        this(message, code, null, Collections.emptyList());
    }

    /**
     * Initializes an AxonServer Query Dispatch Exception using the given {@code message}, {@code code}, {@code source}
     * and {@code details} .
     *
     * @param message a {@link String} describing the exception message
     * @param code    a {@link String} specifying the error code received from the Axon Server
     * @param source  a {@link String} defining the location that originally reported the error
     * @param details a {@link List} of {@link String}s, each describing a single "cause"
     */
    public AxonServerQueryDispatchException(String message, String code, String source, List<String> details) {
        super(message);
        this.code = code;
        this.source = source;
        this.details = details;
    }

    /**
     * Return a {@link String} defining the error code.
     *
     * @return a {@link String} defining the error code
     */
    public String code() {
        return code;
    }

    /**
     * Return a {@link String} defining the source where the error originated.
     *
     * @return a {@link String} defining the source where the error originated
     */
    public String source() {
        return source;
    }

    /**
     * Return a {@link List} of {@link String}s, each describing a single "cause".
     *
     * @return a {@link List} of {@link String}s, each describing a single "cause"
     */
    public List<String> details() {
        return details;
    }
}
