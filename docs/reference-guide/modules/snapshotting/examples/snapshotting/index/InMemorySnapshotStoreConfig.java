/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package snapshotting.index;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;

public class InMemorySnapshotStoreConfig {

    public void configure(ComponentRegistry registry) {
        // tag::in-memory-snapshot-store[]
        registry.registerComponent(
                SnapshotStore.class,
                c -> new InMemorySnapshotStore()
        );
        // end::in-memory-snapshot-store[]
    }
}
