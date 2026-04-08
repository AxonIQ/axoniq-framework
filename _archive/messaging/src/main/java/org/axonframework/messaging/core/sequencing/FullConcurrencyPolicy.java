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

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;


import java.util.Optional;

/**
 * SequencingPolicy that does not enforce any sequencing requirements on message processing.
 *
 * @author Allard Buijze
 * @author Henrique Sena
 * @since 0.3
 */
public class FullConcurrencyPolicy implements SequencingPolicy<Message> {

    /**
     * Singleton instance of the {@code FullConcurrencyPolicy}.
     */
    public static final FullConcurrencyPolicy INSTANCE = new FullConcurrencyPolicy();

    private FullConcurrencyPolicy() {
        // empty private singleton constructor
    }

    @Override
    public Optional<Object> sequenceIdentifierFor(Message message, ProcessingContext context) {
        return Optional.of(message.identifier());
    }
}
