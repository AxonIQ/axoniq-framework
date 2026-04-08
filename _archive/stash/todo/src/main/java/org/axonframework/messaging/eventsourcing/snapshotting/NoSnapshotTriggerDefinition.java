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

import org.axonframework.messaging.eventhandling.EventMessage;


/**
 * Implementation of {@link SnapshotTriggerDefinition} that doesn't trigger snapshots at all.
 */
public enum NoSnapshotTriggerDefinition implements SnapshotTriggerDefinition {

    /**
     * The singleton instance of a {@link NoSnapshotTriggerDefinition}.
     */
    INSTANCE;

    /**
     * A singleton instance of a {@link SnapshotTrigger} that does nothing.
     */
    public static final SnapshotTrigger TRIGGER = new NoSnapshotTrigger();

    @Override
    public SnapshotTrigger prepareTrigger(Class<?> aggregateType) {
        return TRIGGER;
    }

    private static class NoSnapshotTrigger implements SnapshotTrigger {

        @Override
        public void eventHandled(EventMessage msg) {
            // No operation necessary for a no-op implementation.
        }

        @Override
        public void initializationFinished() {
            // No operation necessary for a no-op implementation.
        }
    }
}
