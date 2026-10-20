/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenanteventstorage;

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventsourcing.AggregateBasedAxonServerTenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantEventStorageEngineFactory;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;

public class DeclarativeAggregateBasedEventStorageConfiguration {

    // tag::aggregate-based-factory[]
    public void configure(EventSourcingConfigurer configurer) {
        configurer.componentRegistry(registry -> registry.registerComponent(
                TenantEventStorageEngineFactory.class, // <1>
                config -> new AggregateBasedAxonServerTenantEventStorageEngineFactory( // <2>
                        config.getComponent(AxonServerConnectionManager.class),
                        config.getComponent(EventConverter.class),
                        config.getOptionalComponent(EventTypeResolver.class).orElse(EventTypeResolver.DEFAULT),
                        config.getOptionalComponent(new TypeReference<TenantComponentProvider<Converter>>() {
                        }).orElse(null))
        ));
    }
    // end::aggregate-based-factory[]
}
