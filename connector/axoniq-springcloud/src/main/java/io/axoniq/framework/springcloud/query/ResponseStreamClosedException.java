/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.query;

import org.axonframework.common.AxonException;

/**
 * Raised when a query's responses can no longer be written to the member that asked for them.
 * <p>
 * Not reported back to that member, as there is nowhere left to report it: whatever writes the responses recognises
 * this as the point to stop answering and release the handler's response stream.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class ResponseStreamClosedException extends AxonException {

    private static final long serialVersionUID = 8823167618030431516L;

    /**
     * Constructs a {@code ResponseStreamClosedException} with the given {@code message} and {@code cause}.
     *
     * @param message the message describing what could not be written
     * @param cause   the failure that ended the response stream
     */
    public ResponseStreamClosedException(String message, Throwable cause) {
        super(message, cause);
    }
}
