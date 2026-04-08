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

package org.axonframework.eventsourcing.eventstore;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.Objects;
import java.util.Set;

/**
 * Implementation of the {@link TagResolver} that resolves {@link Tag Tags} based on a metadata key from an
 * {@link EventMessage}.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
public class MetadataBasedTagResolver implements TagResolver {

    private final String metadataKey;

    /**
     * Constructs a {@code MetadataBasedTagResolver} using the given metadata key.
     *
     * @param metadataKey The key to extract the tag value from the event's metadata.
     */
    public MetadataBasedTagResolver(String metadataKey) {
        this.metadataKey = Objects.requireNonNull(metadataKey, "MetadataKey cannot be null");
    }

    @Override
    public Set<Tag> resolve(EventMessage event) {
        var tagValue = event.metadata().get(metadataKey);
        return tagValue == null ? Set.of() : Set.of(new Tag(metadataKey, tagValue));
    }
}