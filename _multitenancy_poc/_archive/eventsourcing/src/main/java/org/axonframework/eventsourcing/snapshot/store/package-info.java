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
 * Store-backed snapshotting implementations for event-sourced entities.
 * <p>
 * This package contains the default {@link org.axonframework.eventsourcing.snapshot.api.Snapshotter}
 * implementation that persists snapshots to a {@link SnapshotStore}, along with supporting interfaces
 * such as {@link SnapshotStore} and {@link SnapshotPolicy}.
 * <p>
 * Users can:
 * <ul>
 *     <li>Use {@link StoreBackedSnapshotter} as the default snapshotter</li>
 *     <li>Provide their own {@link SnapshotStore} implementation</li>
 *     <li>Provide custom {@link org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy} implementations
 *         to control snapshot creation</li>
 * </ul>
 * <p>
 * This package is self-contained and modular. It builds on the {@code api} package but is
 * independent of concrete storage backends, which are provided in other packages or modules.
 */
@NullMarked
package org.axonframework.eventsourcing.snapshot.store;

import org.jspecify.annotations.NullMarked;
