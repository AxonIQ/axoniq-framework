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
