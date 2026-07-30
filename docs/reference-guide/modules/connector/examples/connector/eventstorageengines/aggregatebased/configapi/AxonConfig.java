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
