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

import java.util.Set;

/**
 * Interface describing a condition that the type and tags of event messages must match against in order to be
 * relevant.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
public interface EventsCondition {

    /**
     * The set of criteria against which events must match.
     *
     * @return The {@link EventCriteria} used to match this condition against.
     */
    EventCriteria criteria();

    /**
     * Indicates whether the criteria defined in this condition matches against the given {@code type} and
     * {@code tags}.
     * <p>
     * More specifically, this condition matches if any of the provided criteria match the given {@code type} and
     * {@code tags}, or if no criteria have been provided at all.
     * <p>
     * See {@link EventCriteria} for more details on matching tags.
     *
     * @param type The type of the event to validate against.
     * @param tags The tags of an event message to match.
     * @return {@code true} if given type and tags match, otherwise {@code false}.
     * @see EventCriteria
     */
    default boolean matches(QualifiedName type, Set<Tag> tags) {
        return criteria().matches(type, tags);
    }
}
