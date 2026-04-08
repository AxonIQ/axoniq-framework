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

/**
 * Public API for snapshotting of event-sourced entities.
 * <p>
 * This package contains the interfaces and records that define the contract
 * for snapshot management, independent of any particular storage or implementation.
 * <p>
 * Users can implement their own {@link org.axonframework.eventsourcing.snapshot.api.Snapshotter}
 * or rely on default implementations in the {@code store} package.
 *
 * <ul>
 *     <li>{@link org.axonframework.eventsourcing.snapshot.api.Snapshotter} - the main interface for managing snapshots</li>
 *     <li>{@link org.axonframework.eventsourcing.snapshot.api.Snapshot} - represents a snapshot of an entity at a specific position</li>
 *     <li>{@link org.axonframework.eventsourcing.snapshot.api.EvolutionResult} - metrics collected during entity sourcing</li>
 * </ul>
 */
@NullMarked
package org.axonframework.eventsourcing.snapshot.api;

import org.jspecify.annotations.NullMarked;
