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


import java.util.Objects;
import java.util.Optional;

/**
 * Implementation of {@link SequencingPolicy} that combines two policies in a fallback pattern. When the primary policy
 * fails to determine a sequence identifier for a message (returns {@code Optional.empty()}), this implementation will
 * delegate to a secondary fallback policy.
 * <p>
 * This allows for composing sequencing strategies where certain message types might be handled by specialized policies,
 * falling back to more generic approaches when specialized sequencing fails.
 *
 * @param <M> the type of message to sequence
 * @author Mateusz Nowak
 * @since 5.0.0
 */
public class HierarchicalSequencingPolicy<M extends Message> implements SequencingPolicy<M> {

    private final SequencingPolicy<? super M> primary;
    private final SequencingPolicy<? super M> secondary;

    /**
     * Initializes a new instance with the given primary {@code delegate} and {@code fallback} policies.
     *
     * @param primary   The primary policy to attempt sequence identification with first, not {@code null}.
     * @param secondary The fallback policy to use when the delegate fails, not {@code null}.
     * @throws NullPointerException When either the {@code delegate} or {@code fallback} is {@code null}.
     */
    public HierarchicalSequencingPolicy(SequencingPolicy<? super M> primary, SequencingPolicy<? super M> secondary) {
        this.primary = Objects.requireNonNull(primary, "Primary may not be null.");
        this.secondary = Objects.requireNonNull(secondary, "Secondary may not be null.");
    }

    @Override
    public Optional<Object> sequenceIdentifierFor(M message, ProcessingContext context) {
        return primary.sequenceIdentifierFor(message, context)
                      .or(() -> secondary.sequenceIdentifierFor(message, context));
    }
}