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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@code TagResolver} that combines the results of multiple other tag resolvers. When multiple resolvers provide the
 * same tags, they are merged into a single set.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
public class MultiTagResolver implements TagResolver {

    private final List<? extends TagResolver> delegates;

    /**
     * Initialize the tag resolver, delegating to given {@code tagResolvers}.
     *
     * @param tagResolvers The resolvers to delegate to.
     */
    public MultiTagResolver(List<? extends TagResolver> tagResolvers) {
        this.delegates = List.copyOf(tagResolvers);
    }

    /**
     * Initialize the tag resolver, delegating to given {@code tagResolvers}.
     *
     * @param tagResolvers The resolvers to delegate to.
     */
    public MultiTagResolver(TagResolver... tagResolvers) {
        this.delegates = List.of(tagResolvers);
    }

    @Override
    public Set<Tag> resolve(EventMessage event) {
        Set<Tag> tags = new HashSet<>();
        for (TagResolver delegate : delegates) {
            tags.addAll(delegate.resolve(event));
        }
        return tags;
    }
}