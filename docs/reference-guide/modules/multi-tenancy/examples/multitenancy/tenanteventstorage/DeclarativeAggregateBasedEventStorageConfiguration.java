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

package multitenancy.tenanteventstorage;

import io.axoniq.framework.messaging.multitenancy.axonserver.eventsourcing.AggregateBasedAxonServerTenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantEventStorageEngineFactory;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;

public class DeclarativeAggregateBasedEventStorageConfiguration {

    // tag::aggregate-based-factory[]
    public void configure(EventSourcingConfigurer configurer) {
        configurer.componentRegistry(registry -> registry.registerComponent(
                TenantEventStorageEngineFactory.class,                             // <1>
                AggregateBasedAxonServerTenantEventStorageEngineFactory::new       // <2>
        ));
    }
    // end::aggregate-based-factory[]
}
