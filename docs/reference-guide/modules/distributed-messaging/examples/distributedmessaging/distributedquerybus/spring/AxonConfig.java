/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package distributedmessaging.distributedquerybus.spring;

// tag::query-bus-configuration-bean[]
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBusConfiguration;
import io.axoniq.framework.messaging.queryhandling.distributed.LocalQueryDispatchPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AxonConfig {

    @Bean
    public DistributedQueryBusConfiguration queryBusConfiguration() {
        return DistributedQueryBusConfiguration.DEFAULT.queryThreads(20);
    }

    @Bean
    public LocalQueryDispatchPredicate localQueryDispatchPredicate() {
        // Prefer the local handler for every query that this node can handle.
        return (query, context) -> true;
    }
}
// end::query-bus-configuration-bean[]
