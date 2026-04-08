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

package org.axonframework.eventsourcing;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.StoreBackedSnapshotter;

/**
 * Tests the {@link StoreBackedSnapshotter} with an {@link InMemorySnapshotStore}.
 *
 * @author John Hendrikx
 */
public class InMemoryStoreBackedSnapshotterIT extends StoreBackedSnapshotterTestSuite {
    @Override
    protected void registerComponents(ComponentRegistry registry) {
        registry.registerComponent(SnapshotStore.class, c -> new InMemorySnapshotStore());
    }
}
