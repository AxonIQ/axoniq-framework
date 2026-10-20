/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.task.TaskInstanceId;

import static java.lang.String.format;

/**
 * TaskInstanceId implementation representing a scheduled Deadline task.
 *
 * @author Gerard Klijs
 * @since 4.8.0
 */
@SuppressWarnings("Duplicates")
public class DbSchedulerDeadlineToken implements TaskInstanceId {

    static final String TASK_NAME = "AxonDeadline";

    private final String id;

    /**
     * Initialize a token for the given {@code id}.
     *
     * @param id the identifier used when registering the job with db-scheduler
     */
    public DbSchedulerDeadlineToken(String id) {
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
        final DbSchedulerDeadlineToken other = (DbSchedulerDeadlineToken) obj;
        return this.id.equals(other.id);
    }

    @Override
    public String getTaskName() {
        return TASK_NAME;
    }

    @Override
    public String getId() {
        return id;
    }
}
