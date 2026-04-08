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

package org.axonframework.messaging.eventsourcing.snapshotting;

import org.axonframework.messaging.eventhandling.DomainEventMessage;
import org.axonframework.messaging.eventhandling.EventMessage;


/**
 * Interface describing a mechanism that keeps track of an Aggregate's activity in order to trigger a snapshot. The
 * trigger is notified of events handled by the aggregate and the time at which initialization of "current state" has
 * been completed.
 * <p>
 * It is up to the Trigger implementation to decide when, how and where a snapshot is triggered.
 * <p>
 * SnapshotTrigger implementations do not need to be thread-safe.
 * <p>
 * The trigger instance is attached to the aggregate instance which it monitors, causing it to be cached and potentially
 * serialized alongside it.
 *
 * @see SnapshotTriggerDefinition
 */
public interface SnapshotTrigger {

    /**
     * Invoked when an event is handled by an aggregate. While these messages usually extend
     * {@link DomainEventMessage}, this is not guaranteed. The given message can either
     * be a historic Message from the Event Store, or one that has been just applied by the aggregate.
     *
     * @param msg The message handled by the aggregate
     */
    void eventHandled(EventMessage msg);

    /**
     * Invoked when the initialization of the aggregate based on passed events is completed. Any subsequent invocation
     * of {@link #eventHandled(EventMessage)} involves an event being applied on "current state".
     */
    void initializationFinished();
}
