package connector.eventstorageengines.aggregatebased.configapi;

// tag::aggregate-based-config-api[]
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.AggregateBasedAxonServerEventStorageEngine;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

public class AxonConfig {

    public void configureStorageEngine(EventSourcingConfigurer configurer) {
        configurer.registerEventStorageEngine(config -> {
            AxonServerConnectionManager connectionManager = config.getComponent(AxonServerConnectionManager.class);
            // AxonServerConnectionManager#getConnection returns a connection to the default context.
            // Use AxonServerConnectionManager#getConnection(String) to retrieve a connection to another context.
            return new AggregateBasedAxonServerEventStorageEngine(
                    connectionManager.getConnection(),
                    config.getComponent(EventConverter.class)
            );
        });
    }
}
// end::aggregate-based-config-api[]
