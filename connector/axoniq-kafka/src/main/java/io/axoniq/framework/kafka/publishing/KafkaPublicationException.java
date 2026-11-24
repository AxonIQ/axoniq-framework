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

package io.axoniq.framework.kafka.publishing;

import org.axonframework.common.AxonTransientException;

/**
 * Raised when a {@link KafkaEventSink} fails to publish events to Kafka, for example because a broker is not
 * reachable. Publishing again may succeed, so this exception is transient.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public class KafkaPublicationException extends AxonTransientException {

    /**
     * Constructs an exception with the given {@code message} and {@code cause}.
     *
     * @param message the message describing the failed publication
     * @param cause   the failure reported by Kafka
     */
    public KafkaPublicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
