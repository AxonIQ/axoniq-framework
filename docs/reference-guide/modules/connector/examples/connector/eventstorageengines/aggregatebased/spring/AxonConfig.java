package connector.eventstorageengines.aggregatebased.spring;

import org.springframework.context.annotation.Configuration;

// tag::aggregate-based-spring[]
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.AggregateBasedAxonServerEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.springframework.context.annotation.Bean;

@Configuration
public class AxonConfig {

    @Bean
    public EventStorageEngine storageEngine(AxonServerConnectionManager connectionManager,
                                            EventConverter eventConverter) {
        // AxonServerConnectionManager#getConnection returns a connection to the default context.
        // Use AxonServerConnectionManager#getConnection(String) to retrieve a connection to another context.
        return new AggregateBasedAxonServerEventStorageEngine(
                connectionManager.getConnection(),
                eventConverter
        );
    }
}
// end::aggregate-based-spring[]
