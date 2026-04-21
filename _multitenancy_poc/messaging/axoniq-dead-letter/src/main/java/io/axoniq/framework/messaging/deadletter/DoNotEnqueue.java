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

package io.axoniq.framework.messaging.deadletter;

import org.axonframework.messaging.core.Message;

import java.util.Optional;

/**
 * An {@link EnqueueDecision} stating a {@link DeadLetter dead letter} should not be enqueued.
 *
 * @param <M> An implementation of {@link Message} contained in the {@link DeadLetter dead letter} that is decided on.
 * @author Steven van Beelen
 * @since 4.6.0
 */
public class DoNotEnqueue<M extends Message> implements EnqueueDecision<M> {

    @Override
    public boolean shouldEnqueue() {
        return false;
    }

    @Override
    public Optional<Throwable> enqueueCause() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "DoNotEnqueue{}";
    }
}
