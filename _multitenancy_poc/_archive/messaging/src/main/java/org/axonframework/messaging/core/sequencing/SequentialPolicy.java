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

package org.axonframework.messaging.core.sequencing;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;


import java.util.Optional;

/**
 * SequencingPolicy that requires sequential handling of all messages delivered to a message handler.
 *
 * @author Allard Buijze
 * @since 0.3
 */
public class SequentialPolicy implements SequencingPolicy<Message> {

    /**
     * Singleton instance of the {@code SequentialPolicy}.
     */
    public static final SequentialPolicy INSTANCE = new SequentialPolicy();

    /**
     * Object used to represent the full sequential policy.
     * <p>
     * Note: This uses a String constant to provide a consistent {@link Object#hashCode()} and
     * {@link Object#equals(Object)} behaviour across JVM restarts.
     */
    @Internal
    public static final Object FULL_SEQUENTIAL_POLICY = "FULL_SEQUENTIAL_POLICY";

    private SequentialPolicy() {
    }

    @Override
    public Optional<Object> sequenceIdentifierFor(Message message, ProcessingContext context) {
        return Optional.of(FULL_SEQUENTIAL_POLICY);
    }
}
