/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

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
