package connector.distributingevents;

// tag::context-specific-storage-engine[]
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;

public class AxonConfig {

    public EventStorageEngine contextSpecificStorageEngine(Configuration configuration,
                                                           String context) {
        return configuration.getComponent(AxonServerEventStorageEngine.class, "storageEngine@" + context);
    }
}
// end::context-specific-storage-engine[]
