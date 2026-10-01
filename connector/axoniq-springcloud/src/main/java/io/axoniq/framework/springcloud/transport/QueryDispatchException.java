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

package io.axoniq.framework.springcloud.transport;

import org.axonframework.common.AxonException;

/**
 * Raised when a query could not be sent to a member, or its responses could not be read from one.
 * <p>
 * Distinct from the failure of a handler that did run, which arrives as a reported error on the response stream. The
 * connector acts on that distinction: a member that could not be reached is taken out of the routing ring, while a
 * member whose handler rejected a query is left where it is.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class QueryDispatchException extends AxonException {

    private static final long serialVersionUID = 3009371806498561176L;

    /**
     * Constructs a {@code QueryDispatchException} with the given {@code message}.
     *
     * @param message the description of what could not be reached
     */
    public QueryDispatchException(String message) {
        super(message);
    }

    /**
     * Constructs a {@code QueryDispatchException} with the given {@code message} and {@code cause}.
     *
     * @param message the description of what could not be reached
     * @param cause   the failure encountered while reaching the member
     */
    public QueryDispatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
