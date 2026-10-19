/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
