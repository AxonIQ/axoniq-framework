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

package org.axonframework.messaging.eventsourcing;

import org.axonframework.messaging.eventsourcing.snapshotting.NoSnapshotTriggerDefinition;
import org.axonframework.messaging.eventsourcing.snapshotting.SnapshotTrigger;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class NoSnapshotTriggerDefinitionTest {

    @Test
    void triggerDefinitionReturnsSameInstance() {
        SnapshotTrigger instance1 = NoSnapshotTriggerDefinition.INSTANCE.prepareTrigger(Object.class);
        SnapshotTrigger instance2 = NoSnapshotTriggerDefinition.INSTANCE.prepareTrigger(Object.class);
        SnapshotTrigger instance3 = NoSnapshotTriggerDefinition.INSTANCE.prepareTrigger(Object.class);

        assertSame(instance1, instance2);
        assertSame(instance1, instance3);
        assertSame(instance2, instance3);
    }
}
