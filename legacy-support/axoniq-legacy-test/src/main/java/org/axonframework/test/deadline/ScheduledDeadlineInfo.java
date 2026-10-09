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

package org.axonframework.test.deadline;

import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.Scope;

import java.time.Instant;
import java.util.Objects;

/**
 * Holds the data regarding deadline schedule.
 *
 * @author Milan Savic
 * @author Steven van Beelen
 * @since 3.3
 */
public class ScheduledDeadlineInfo implements Comparable<ScheduledDeadlineInfo> {

    private final Instant scheduleTime;
    private final String deadlineName;
    private final String scheduleId;
    private final int counter;
    private final DeadlineMessage deadlineMessage;
    private final ScopeDescriptor deadlineScope;

    /**
     * Instantiates a ScheduledDeadlineInfo.
     *
     * @param scheduleTime    the time as an {@link Instant} at which the deadline is scheduled
     * @param deadlineName    a {@link String} denoting the name of the deadline; can be used together with the
     *                        {@code scheduleId} to cancel the deadline
     * @param scheduleId      a {@link String} identifier representing the scheduled deadline; can be used together
     *                        with the {@code deadlineName} to cancel the deadline
     * @param counter         used to differentiate two deadlines scheduled at the same time
     * @param deadlineMessage the deadline message of the scheduled deadline
     * @param deadlineScope   a description of the {@link Scope} in which the deadline is
     *                        scheduled
     */
    public ScheduledDeadlineInfo(Instant scheduleTime,
                                 String deadlineName,
                                 String scheduleId,
                                 int counter,
                                 DeadlineMessage deadlineMessage,
                                 ScopeDescriptor deadlineScope) {
        this.scheduleTime = scheduleTime;
        this.deadlineName = deadlineName;
        this.scheduleId = scheduleId;
        this.counter = counter;
        this.deadlineMessage = deadlineMessage;
        this.deadlineScope = deadlineScope;
    }

    /**
     * Creates a new instance of scheduled deadline info with new {@code deadlineMessage}. Other fields are the
     * same.
     *
     * @param deadlineMessage new deadline message
     * @return new instance with given {@code deadlineMessage}
     */
    public ScheduledDeadlineInfo recreateWithNewMessage(DeadlineMessage deadlineMessage) {
        return new ScheduledDeadlineInfo(scheduleTime,
                                         deadlineName,
                                         scheduleId,
                                         counter,
                                         deadlineMessage,
                                         deadlineScope);
    }

    /**
     * Retrieve the time as an {@link Instant} at which the deadline is scheduled.
     *
     * @return the time as an {@link Instant} at which the deadline is scheduled
     */
    public Instant getScheduleTime() {
        return scheduleTime;
    }

    /**
     * Retrieve a {@link String} denoting the name of the deadline; can be used together with the {@code scheduleId} to
     * cancel the deadline.
     *
     * @return a {@link String} denoting the name of the deadline; can be used together with the {@code scheduleId} to
     * cancel the deadline
     */
    public String getDeadlineName() {
        return deadlineName;
    }

    /**
     * Retrieve a {@link String} identifier representing the scheduled deadline; can be used together with the
     * {@code deadlineName} to cancel the deadline.
     *
     * @return a {@link String} identifier representing the scheduled deadline; can be used together with the
     * {@code deadlineName} to cancel the deadline
     */
    public String getScheduleId() {
        return scheduleId;
    }

    /**
     * Retrieve the counter used to differentiate two deadlines scheduled at the same time.
     *
     * @return the counter used to differentiate two deadlines scheduled at the same time
     */
    public int getCounter() {
        return counter;
    }

    /**
     * Retrieve a description of the {@link Scope} in which the deadline is scheduled.
     *
     * @return a description of the {@link Scope} in which the deadline is scheduled
     */
    public ScopeDescriptor getDeadlineScope() {
        return deadlineScope;
    }

    /**
     * Retrieve a {@link DeadlineMessage} constructed out of the {@code deadlineName} and {@code deadlineInfo}.
     *
     * @return a {@link DeadlineMessage} constructed out of the {@code deadlineName} and {@code deadlineInfo}
     */
    public DeadlineMessage deadlineMessage() {
        return deadlineMessage;
    }

    @Override
    public int compareTo(ScheduledDeadlineInfo other) {
        if (scheduleTime.equals(other.scheduleTime)) {
            return Integer.compare(counter, other.counter);
        }
        return scheduleTime.compareTo(other.scheduleTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScheduledDeadlineInfo that = (ScheduledDeadlineInfo) o;
        return counter == that.counter &&
                Objects.equals(scheduleTime, that.scheduleTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scheduleTime, counter);
    }
}
