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

package org.axonframework.messaging.eventstreaming;

import org.axonframework.messaging.core.QualifiedName;

import java.util.Collections;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Filtered {@link EventCriteria} that matches events based on only their tags. The tags of the event must contain all
 * tags in the {@code tags} set to match.
 * <p>
 * You can limit the types of events to be matched by using the {@link AnyEvent#andBeingOneOfTypes(Set)} method, which
 * will return a new {@link EventCriteria} that matches only the specified types.
 *
 *
 * @param tags The tags that events are expected to have.
 * @author Steven van Beelen
 * @author Allard Buijze
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
record TagFilteredEventCriteria(Set<Tag> tags)
        implements EventCriteria, EventCriterion, EventTypeRestrictableEventCriteria {

    /**
     * Constructs an {@link EventCriteria} that matches against the given {@code tags}. If the {@code tags} set is
     * empty, the criteria will match against any tags.
     *
     * @param tags The tags that events are expected to have.
     */
    TagFilteredEventCriteria(Set<Tag> tags) {
        this.tags = Set.copyOf(requireNonNull(tags, "The tags cannot be null"));
    }

    @Override
    public Set<EventCriterion> flatten() {
        if (tags.isEmpty()) {
            return Collections.emptySet();
        }
        return Set.of(this);
    }

    @Override
    public Set<QualifiedName> types() {
        return Collections.emptySet();
    }

    @Override
    public Set<Tag> tags() {
        return tags;
    }

    @Override
    public boolean matches(QualifiedName type, Set<Tag> tags) {
        return matchesTags(tags);
    }

    private boolean matchesTags(Set<Tag> tags) {
        return tags.containsAll(this.tags);
    }

    @Override
    public EventCriteria or(EventCriteria criteria) {
        if (criteria instanceof AnyEvent) {
            return criteria;
        }
        if (criteria.equals(this)) {
            return this;
        }

        // If the given criteria is an OrEventCriteria, let it be flattened
        if (criteria instanceof OrEventCriteria orEventCriteria) {
            return orEventCriteria.or(this);
        }

        return new OrEventCriteria(Set.of(this, criteria));
    }

    @Override
    public boolean hasCriteria() {
        return !tags.isEmpty();
    }

    @Override
    public EventCriteria andBeingOneOfTypes(Set<QualifiedName> types) {
        return new TagAndTypeFilteredEventCriteria(types, tags);
    }
}
