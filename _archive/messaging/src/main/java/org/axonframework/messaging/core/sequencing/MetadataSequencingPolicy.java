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

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;


import java.util.Optional;

import static org.axonframework.common.BuilderUtils.assertNonBlank;

/**
 * A {@link SequencingPolicy} implementation that extracts the sequence identifier from the {@link Message}'s
 * {@link Metadata}, based on a given {@code metadataKey}. In the absence of the given {@code metadataKey} on the
 * {@link Metadata}, the {@link Optional#empty()} is returned.
 *
 * @author Lucas Campos
 * @since 4.6.0
 */
public class MetadataSequencingPolicy implements SequencingPolicy<Message> {

    private final String metadataKey;

    /**
     * Instantiate a {@code MetadataSequencingPolicy}.
     * <p>
     * Will assert that the {@code metadataKey} is not {@code null} and will throw an {@link AxonConfigurationException}
     * if this is the case.
     *
     * @param metadataKey The key to be used as a lookup for the property to be used as the Sequence Policy.
     */
    public MetadataSequencingPolicy(String metadataKey) {
        this.metadataKey = assertNonBlank(metadataKey, "MetadataKey value may not be null or blank.");
    }

    @Override
    public Optional<Object> sequenceIdentifierFor(Message message, ProcessingContext context) {
        return Optional.ofNullable(message.metadata().get(metadataKey));
    }
}
