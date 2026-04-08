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
 * Describes a singular, non-nested criteria for filtering events. Can be acquired by using the
 * {@link EventCriteria#flatten()} method.
 *
 * @author Mitchell Herrijgers
 * @see EventCriteria
 * @since 5.0.0
 */
public sealed interface EventCriterion extends EventCriteria
        permits TagAndTypeFilteredEventCriteria, TagFilteredEventCriteria {

    /**
     * A {@link Set} of {@link QualifiedName QualifiedNames} containing all the types of events applicable for sourcing,
     * streaming, or appending events.
     *
     * @return The {@link Set} of {@link QualifiedName QualifiedNames} containing all the types of events applicable for
     * sourcing, streaming, or appending events.
     */
    Set<QualifiedName> types();

    /**
     * A {@link Set} of {@link Tag Tags} applicable for sourcing, streaming, or appending events. A {@code Tag} can, for
     * example, refer to an entities' (aggregate) identifier name and value.
     *
     * @return The {@link Set} of {@link Tag Tags} applicable for sourcing, streaming, or appending events.
     */
    Set<Tag> tags();
}
