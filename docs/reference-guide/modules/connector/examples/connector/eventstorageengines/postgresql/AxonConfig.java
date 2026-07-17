package connector.eventstorageengines.postgresql;

// tag::postgresql-config-api[]
import io.axoniq.framework.postgresql.PostgresqlEventStorageEngine;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import javax.sql.DataSource;

public class AxonConfig {

    public void configureStorageEngine(EventSourcingConfigurer configurer) {
        configurer.registerEventStorageEngine(config -> new PostgresqlEventStorageEngine(
                config.getComponent(DataSource.class),
                config.getComponent(EventConverter.class)
        ));
    }
}
// end::postgresql-config-api[]
