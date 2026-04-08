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

package org.axonframework.modelling.command;

import org.axonframework.messaging.core.Message;

import java.util.stream.Stream;

/**
 * Forward all messages {@code T} regardless of their set up.
 *
 * @param <T> the implementation {@code T} of the {@link Message} being filtered.
 * @author Steven van Beelen
 * @since 3.1
 */
public class ForwardToAll<T extends Message> implements ForwardingMode<T> {

    @Override
    public <E> Stream<E> filterCandidates(T message, Stream<E> candidates) {
        return candidates;
    }
}
