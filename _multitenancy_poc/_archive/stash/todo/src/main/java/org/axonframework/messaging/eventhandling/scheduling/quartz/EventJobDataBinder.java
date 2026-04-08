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

package org.axonframework.messaging.eventhandling.scheduling.quartz;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.quartz.JobDataMap;

/**
 * Strategy towards reading/writing an Axon EventMessage from/to a Quartz {@link JobDataMap}.
 * <p>
 * Implementors may choose how to store the event message as serializable data in {@link JobDataMap}.
 * This is useful when one does not want to be limited to event payloads requiring to be {@link java.io.Serializable}
 * (used by Quartz job data map conversion).
 * </p>
 *
 * @see QuartzEventScheduler
 * @see FireEventJob
 */
public interface EventJobDataBinder {

    /**
     * Write an {@code eventMessage} (or its payload) to a {@link JobDataMap}.
     *
     * @param eventMessage to write to the {@link JobDataMap}
     * @return {@link JobDataMap} written to (must not be null)
     */
    JobDataMap toJobData(Object eventMessage);

    /**
     * Read an {@link EventMessage} (or its payload) from the {@link JobDataMap}.
     *
     * @param jobData to read from
     * @return event message (or its payload)
     */
    Object fromJobData(JobDataMap jobData);

}
