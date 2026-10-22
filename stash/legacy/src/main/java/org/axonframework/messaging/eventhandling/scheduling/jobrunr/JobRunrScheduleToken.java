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

package org.axonframework.messaging.eventhandling.scheduling.jobrunr;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.axonframework.messaging.eventhandling.scheduling.ScheduleToken;

import java.beans.ConstructorProperties;
import java.util.UUID;

import static java.lang.String.format;

/**
 * ScheduleToken implementation representing a scheduled JobRunr Job.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class JobRunrScheduleToken implements ScheduleToken {

    private final UUID jobIdentifier;

    /**
     * Initialize a token for the given {@code jobIdentifier}.
     *
     * @param jobIdentifier The identifier used when registering the job with JobRunr.
     */
    @JsonCreator
    @ConstructorProperties({"jobIdentifier", "groupIdentifier"})
    public JobRunrScheduleToken(@JsonProperty("jobIdentifier") UUID jobIdentifier) {
        this.jobIdentifier = jobIdentifier;
    }

    /**
     * Returns the JobRunr job identifier.
     *
     * @return the JobRunr job identifier
     */
    public UUID getJobIdentifier() {
        return jobIdentifier;
    }

    @Override
    public String toString() {
        return format("JobRunr Schedule token for job [%s]", jobIdentifier);
    }

    @Override
    public int hashCode() {
        return jobIdentifier.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final JobRunrScheduleToken other = (JobRunrScheduleToken) obj;
        return this.jobIdentifier.equals(other.jobIdentifier);
    }
}
