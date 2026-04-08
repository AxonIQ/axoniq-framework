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
 * Variant of the {@link OrEventCriteria} that can still be restricted on event types, as specified by
 * {@link EventTypeRestrictableEventCriteria}. Any {@link EventTypeRestrictableEventCriteria} methods called on this
 * instance will be applied to the {@code buildingCriteria} and the result will be OR'ed with the
 * {@code otherCriteria}.
 * <p>
 * For all intents and purposes, this class behaves like an {@link OrEventCriteria}.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
final class EventTypeRestrictableOrEventCriteria extends OrEventCriteria implements EventTypeRestrictableEventCriteria {

    private final EventTypeRestrictableEventCriteria buildingCriteria;
    private final EventCriteria otherCriteria;

    /**
     * Constructs an {@code OrEventCriteria} that will match the {@code buildingCriteria} and the {@code otherCriteria},
     * but of which the {@code buildingCriteria} can still be restricted on event types.
     *
     * @param buildingCriteria The {@link EventCriteria} that is being built. The
     *                         {@link EventTypeRestrictableEventCriteria} methods will apply to this instance.
     * @param otherCriteria    The {@link EventCriteria} that is being OR'ed with the {@code buildingCriteria}. This
     *                         instance will not be modified.
     */
    EventTypeRestrictableOrEventCriteria(EventTypeRestrictableEventCriteria buildingCriteria,
                                         EventCriteria otherCriteria) {
        super(Set.of(buildingCriteria, otherCriteria));
        this.buildingCriteria = buildingCriteria;
        this.otherCriteria = otherCriteria;
    }

    @Override
    public EventCriteria andBeingOneOfTypes(Set<QualifiedName> types) {
        EventCriteria builtEventCriteria = buildingCriteria.andBeingOneOfTypes(types);
        return otherCriteria.or(builtEventCriteria);
    }
}
