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

package org.axonframework.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import org.junit.jupiter.api.*;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DbScheduledDeadlineTokenTest {

    @Test
    void equalsIfSameUuidIsUsed() {
        String uuid = UUID.randomUUID().toString();
        TaskInstanceId one = new DbSchedulerDeadlineToken(uuid);
        TaskInstanceId other = new DbSchedulerDeadlineToken(uuid);

        assertEquals(one, other);
        assertEquals(one.toString(), other.toString());
        assertEquals(one.hashCode(), other.hashCode());
    }

    @Test
    void notEqualsIfDifferentUuidIsUsed() {
        TaskInstanceId one = new DbSchedulerDeadlineToken(UUID.randomUUID().toString());
        TaskInstanceId other = new DbSchedulerDeadlineToken(UUID.randomUUID().toString());

        assertNotEquals(one, other);
        assertNotEquals(one.toString(), other.toString());
        assertNotEquals(one.hashCode(), other.hashCode());
    }
}
