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


/**
 * Interface describing instances that are capable of creating snapshot events for aggregates. Although snapshotting is
 * typically an asynchronous process, implementations may to choose to create snapshots in the calling thread.
 *
 * @author Allard Buijze
 * @since 0.6
 */
public interface Snapshotter {

    /**
     * Schedules snapshot taking for an aggregate with given {@code aggregateIdentifier}. The implementation may choose
     * to process this call synchronously (i.e. in the caller's thread), asynchronously, or ignore the call altogether.
     *
     * @param aggregateType       the type of the aggregate to take the snapshot for
     * @param aggregateIdentifier The identifier of the aggregate to take the snapshot for
     */
    void scheduleSnapshot(Class<?> aggregateType, String aggregateIdentifier);
}
