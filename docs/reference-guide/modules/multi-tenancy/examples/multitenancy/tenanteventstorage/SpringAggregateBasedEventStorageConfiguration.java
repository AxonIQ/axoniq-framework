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

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventsourcing.AggregateBasedAxonServerTenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantEventStorageEngineFactory;
import org.axonframework.common.TypeReference;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.springframework.context.annotation.Bean;

@org.springframework.context.annotation.Configuration
public class SpringAggregateBasedEventStorageConfiguration {

    // tag::aggregate-based-factory[]
    @Bean
    public TenantEventStorageEngineFactory tenantEventStorageEngineFactory(Configuration configuration) {
        return new AggregateBasedAxonServerTenantEventStorageEngineFactory( // <1>
                configuration.getComponent(AxonServerConnectionManager.class),
                configuration.getComponent(EventConverter.class),
                configuration.getOptionalComponent(EventTypeResolver.class).orElse(EventTypeResolver.DEFAULT),
                configuration.getOptionalComponent(new TypeReference<TenantComponentProvider<Converter>>() {
                }).orElse(null)
        );
    }
    // end::aggregate-based-factory[]
}
