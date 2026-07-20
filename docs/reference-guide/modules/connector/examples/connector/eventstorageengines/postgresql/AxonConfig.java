/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

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
