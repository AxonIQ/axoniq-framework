/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.query;

import org.axonframework.common.AxonNonTransientException;

/**
 * Raised when a response to a query cannot be converted to the format it is sent to another member in.
 * <p>
 * Non-transient by nature: whatever made the response unconvertible, such as a payload the converter cannot serialize,
 * will make an identical response unconvertible too.
 *
 * @author Steven van Beelen
 * @since 5.4.3
 */
public class UnconvertibleResponseException extends AxonNonTransientException {

    private static final long serialVersionUID = 2862146397452790421L;

    /**
     * Constructs an {@code UnconvertibleResponseException} with the given {@code message} and {@code cause}.
     *
     * @param memberName   the name of the member that could not convert the response
     * @param queryType    the type of the query the response answers
     * @param responseType the type of the response that could not be converted
     * @param cause        the failure encountered while converting
     */
    public UnconvertibleResponseException(String memberName, String queryType, String responseType, Exception cause) {
        super(
                "Member [" + memberName + "] could not convert a response of type [" + responseType
                        + "] to the answer to query of type [" + queryType + "].",
                cause
        );
    }
}
