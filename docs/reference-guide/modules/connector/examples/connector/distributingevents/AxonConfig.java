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
