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

package io.axoniq.framework.kafka.codec;

import org.axonframework.common.AxonNonTransientException;

/**
 * Raised when a consumed Kafka record cannot be decoded into an event.
 * <p>
 * This happens when a record carries an event whose contents cannot be read, or when the
 * {@link UndecodableRecordPolicy#FAIL} policy rejects a record that carries no event. Consuming the same record again
 * gives the same result, so this exception is not transient.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public class UndecodableRecordException extends AxonNonTransientException {

    /**
     * Constructs an exception with the given {@code message}.
     *
     * @param message the message describing the record and why it cannot be decoded
     */
    public UndecodableRecordException(String message) {
        super(message);
    }

    /**
     * Constructs an exception with the given {@code message} and {@code cause}.
     *
     * @param message the message describing the record and why it cannot be decoded
     * @param cause   the failure that prevented decoding the record
     */
    public UndecodableRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
