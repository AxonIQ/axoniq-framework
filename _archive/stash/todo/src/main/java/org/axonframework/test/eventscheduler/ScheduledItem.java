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

package org.axonframework.test.eventscheduler;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Instant;


/**
 * Interface describing an event to be scheduled at a given date and time.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public interface ScheduledItem {

    /**
     * The time the event was scheduled for publication.
     *
     * @return time the event was scheduled for publication
     */
    Instant getScheduleTime();

    /**
     * The Event scheduled for publication.
     *
     * @return the Event scheduled for publication
     */
    EventMessage getEvent();
}
