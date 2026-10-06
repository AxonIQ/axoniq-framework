/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import org.junit.jupiter.api.*;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DbScheduledDeadlineTokenTest {

    @Test
    void equalsIfSameUuidIsUsed() {
        String uuid = UUID.randomUUID().toString();
        TaskInstanceId one = new DbSchedulerDeadlineToken(uuid);
        TaskInstanceId other = new DbSchedulerDeadlineToken(uuid);

        assertThat(one).isEqualTo(other);
        assertThat(one.toString()).isEqualTo(other.toString());
        assertThat(one.hashCode()).isEqualTo(other.hashCode());
    }

    @Test
    void notEqualsIfDifferentUuidIsUsed() {
        TaskInstanceId one = new DbSchedulerDeadlineToken(UUID.randomUUID().toString());
        TaskInstanceId other = new DbSchedulerDeadlineToken(UUID.randomUUID().toString());

        assertThat(one).isNotEqualTo(other);
        assertThat(one.toString()).isNotEqualTo(other.toString());
        assertThat(one.hashCode()).isNotEqualTo(other.hashCode());
    }
}
