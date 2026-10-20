/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package distributedmessaging.distributedquerybus.customize;

// tag::customize-query-bus-configuration[]
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBusConfiguration;
import io.axoniq.framework.messaging.queryhandling.distributed.LocalQueryDispatchPredicate;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

public class AxonConfig {
    public void configureQueryBus(MessagingConfigurer configurer) {
        // Customize the configuration
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT
                .queryThreads(20)          // Set number of query processing threads
                .queryQueueCapacity(2000); // Set queue capacity

        // Register the custom configuration
        configurer.componentRegistry(
                cr -> cr.registerComponent(DistributedQueryBusConfiguration.class, c -> config)
                        .registerComponent(
                                LocalQueryDispatchPredicate.class,
                                c -> (query, context) -> true
                        )
        );
    }
}
// end::customize-query-bus-configuration[]
