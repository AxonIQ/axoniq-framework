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

package org.axonframework.messaging.eventhandling.scheduling.dbscheduler;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import org.axonframework.messaging.eventhandling.scheduling.ScheduleToken;

import java.beans.ConstructorProperties;

import static java.lang.String.format;

/**
 * ScheduleToken implementation representing a scheduled DbScheduler Job.
 *
 * @author Gerard Klijs
 * @since 4.8.0
 */
public class DbSchedulerScheduleToken implements ScheduleToken, TaskInstanceId {

    static final String TASK_NAME = "AxonScheduledEvent";

    private final String id;

    /**
     * Initialize a token for the given {@code id}.
     *
     * @param id The identifier used when registering the job with DbScheduler.
     */
    @JsonCreator
    @ConstructorProperties("id")
    public DbSchedulerScheduleToken(@JsonProperty("id") String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return format("DbScheduler Schedule token for job [%s]", id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final DbSchedulerScheduleToken other = (DbSchedulerScheduleToken) obj;
        return this.id.equals(other.id);
    }

    @JsonIgnore
    @Override
    public String getTaskName() {
        return TASK_NAME;
    }

    @Override
    public String getId() {
        return id;
    }
}
