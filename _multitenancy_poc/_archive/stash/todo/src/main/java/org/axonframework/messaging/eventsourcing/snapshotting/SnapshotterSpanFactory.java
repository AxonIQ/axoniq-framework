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

import org.axonframework.messaging.tracing.Span;

/**
 * Span factory that creates spans for the {@link Snapshotter}.
 * You can customize the spans of the snapshotter by creating your own implementation.
 *
 * @author Mitchell Herrijgers
 * @since 4.9.0
 */
public interface SnapshotterSpanFactory {
    /**
     * Creates a new {@link Span} that represents the scheduling of snapshot creation to the snapshotter's executor.
     *
     * @param aggregateType       The aggregate's type.
     * @param aggregateIdentifier The aggregate's identifier.
     * @return A {@link Span} representing the scheduling of snapshot creation.
     */
    Span createScheduleSnapshotSpan(String aggregateType, String aggregateIdentifier);

    /**
     * Creates a new {@link Span} that represents the actual creation of a snapshot. The creation
     * of a snapshot might be done in a separate thread depending on the implementation of the  {@link Snapshotter}.
     *
     * @param aggregateType       The aggregate's type.
     * @param aggregateIdentifier The aggregate's identifier.
     * @return A {@link Span} representing the creation of a snapshot.
     */
    Span createCreateSnapshotSpan(String aggregateType, String aggregateIdentifier);
}
