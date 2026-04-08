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
import org.jspecify.annotations.NonNull;

import java.util.Optional;

/**
 * {@link SequencingPolicy} that imposes no sequencing at all on the processing of messages. Infrastructure components
 * may decide upon this sequencing policy being present bypassing sequencing infrastructure at all.
 *
 * @author Jakob Hatzl
 * @since 5.0.3
 */
public class NoOpSequencingPolicy implements SequencingPolicy<Message> {

    /**
     * Singleton instance of the {@link NoOpSequencingPolicy}
     */
    public static final NoOpSequencingPolicy INSTANCE = new NoOpSequencingPolicy();

    private NoOpSequencingPolicy() {
        // empty private singleton constructor
    }

    @Override
    public Optional<Object> sequenceIdentifierFor(@NonNull Message message,
                                                  @NonNull ProcessingContext context) {
        return Optional.empty();
    }
}
