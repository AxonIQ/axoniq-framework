/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.query;

import org.axonframework.common.AxonNonTransientException;

/**
 * Raised when a query received from another member cannot be read.
 * <p>
 * Non-transient by nature: whatever made the request unreadable will make an identical one unreadable too. Reported to
 * the member that asked as a {@link QueryErrorCode#QUERY_EXECUTION_NON_TRANSIENT_ERROR}, so that it does not spend a
 * retry on bytes that cannot succeed.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class UnreadableQueryException extends AxonNonTransientException {

    private static final long serialVersionUID = 5178329465517269045L;

    /**
     * Constructs an {@code UnreadableQueryException} with the given {@code message} and {@code cause}.
     *
     * @param message the message describing what could not be read
     * @param cause   the failure encountered while reading
     */
    public UnreadableQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
