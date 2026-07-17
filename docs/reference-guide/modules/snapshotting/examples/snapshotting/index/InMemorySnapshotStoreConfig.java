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
