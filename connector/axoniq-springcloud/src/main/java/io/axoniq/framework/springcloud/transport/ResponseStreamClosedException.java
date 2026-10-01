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
