/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.query;

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
