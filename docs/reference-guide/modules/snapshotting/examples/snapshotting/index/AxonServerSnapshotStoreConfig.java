package snapshotting.index;

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.snapshot.AxonServerSnapshotStore;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;

public class AxonServerSnapshotStoreConfig {

    public void configure(ComponentRegistry registry) {
        // tag::axon-server-snapshot-store[]
        registry.registerComponent(SnapshotStore.class, c -> {
            AxonServerConnectionManager connectionManager =
                    c.getComponent(AxonServerConnectionManager.class);

            return new AxonServerSnapshotStore(
                    connectionManager.getConnection(),
                    c.getComponent(Converter.class)
            );
        });
        // end::axon-server-snapshot-store[]
    }
}
